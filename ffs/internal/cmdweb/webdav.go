package cmdweb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"golang.org/x/net/webdav"
)

var _ webdav.FileSystem = davFS{}
var _ webdav.File = davFile{}

var davFlags struct {
	Writable bool `flag:"writable,Export a writable filesystem"`
	Verbose  bool `flag:"v,Enable verbose logging"`
}

func runWebDAV(env *command.Env, address, rootKey string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		pi, err := filetree.OpenPath(env.Context(), s, rootKey)
		if err != nil {
			return err
		}
		fmt.Fprintf(env, "Resolved %q to %s\n", rootKey, config.FormatKey(pi.FileKey))

		hostPort, servePath := parseAddress(address)
		if hostPort == "" {
			return env.Usagef("address must not be empty")
		}
		var logf func(string, ...any)
		if davFlags.Verbose {
			logf = log.Printf
		}
		srv := &http.Server{
			Addr: hostPort,
			Handler: fixOverwriteHandler{&webdav.Handler{
				Prefix:     servePath,
				FileSystem: davFS{pi: pi, logf: logf, writable: davFlags.Writable},
				LockSystem: webdav.NewMemLS(),
			}},
		}
		go func() {
			<-env.Context().Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv.Shutdown(ctx)
		}()
		fmt.Fprintf(env, "Serving at http://%s%s (writable=%v)\n", hostPort, servePath, davFlags.Writable)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		fmt.Fprintln(env, "Server exited")

		// If the filesystem was writable, flush out any changes to storage.
		// N.B. Background context here as the env context is already done.
		if davFlags.Writable {
			key, err := pi.Flush(context.Background())
			if err != nil {
				return fmt.Errorf("flush file data: %w", err)
			}
			fmt.Printf("flush: %s\n", config.FormatKey(key))
		}
		return nil
	})
}

func parseAddress(s string) (hostPort, path string) {
	hostPort, path, _ = strings.Cut(s, "/")
	if strings.HasPrefix(hostPort, ":") {
		hostPort = "localhost" + hostPort
	}
	if path != "" {
		path = "/" + path
	}
	return
}

type davFS struct {
	pi       *filetree.PathInfo
	logf     func(string, ...any)
	writable bool
}

func (d davFS) logPrintf(msg string, args ...any) func(*error) {
	if d.logf != nil {
		d.logf(msg, args...)
		return func(errp *error) { d.logf("result=%v", *errp) }
	}
	return func(*error) {}
}

// Mkdir implements part of the webdav.FileSystem interface.
func (d davFS) Mkdir(ctx context.Context, name string, perm os.FileMode) (err error) {
	defer d.logPrintf("[fs=%v] mkdir %q %v", d, name, perm)(&err)
	if !d.writable {
		return errReadOnly("mkdir")
	}
	clean := cleanPath(name)
	if _, err := fpath.Open(ctx, d.pi.File, clean); err == nil {
		return pathError("mkdir", name, os.ErrExist)
	}
	if _, err := fpath.Set(ctx, d.pi.File, clean, &fpath.SetOptions{
		Create: false, // do not create intermediates
		File: d.pi.File.New(&file.NewOptions{
			Name: path.Base(clean),
			Stat: &file.Stat{
				Mode:    fs.ModeDir | perm,
				OwnerID: os.Getuid(),
				GroupID: os.Getgid(),
			},
			PersistStat: d.pi.File.Stat().Persistent(),
		}),
	}); err != nil {
		return pathError("mkdir", clean, err)
	}
	return nil
}

func (d davFS) fh(ctx context.Context, f *file.File, writeOK bool) davFile {
	return davFile{ctx: ctx, pi: d.pi, Cursor: f.Cursor(ctx), writable: writeOK}
}

// OpenFile implements part of the [webdav.FileSystem] interface.
func (d davFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (_ webdav.File, err error) {
	defer d.logPrintf("[fs=%v] openfile %q %o %v", d, name, flag, perm)(&err)
	wantWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE) != 0
	if !d.writable && wantWrite {
		return nil, errReadOnly("open")
	}
	if flag&(os.O_SYNC|os.O_APPEND) != 0 {
		return nil, pathError("open", name, os.ErrInvalid)
	}
	if name == "/" {
		return d.fh(ctx, d.pi.File, wantWrite), nil
	}

	// N.B. It is essential that these errors are of type fs.PathError, as the
	// os.Is* functions do not unwrap like errors.Is does.

	var f, dst *file.File
	dir, base := splitPath(name)
	if dir == "" && base == "" {
		dst = d.pi.File
	} else if dst, err = fpath.Open(ctx, d.pi.File, dir); err != nil {
		return nil, pathError("open", dir, err)
	}
	if dst.Child().Has(base) {
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			return nil, pathError("open", name, os.ErrExist)
		}
		f, err = dst.Open(ctx, base)
	} else if flag&os.O_CREATE == 0 {
		return nil, pathError("open", name, os.ErrNotExist)
	} else {
		f = dst.New(&file.NewOptions{
			Name: base,
			Stat: &file.Stat{
				Mode:    perm,
				OwnerID: os.Getuid(),
				GroupID: os.Getgid(),
			},
			PersistStat: dst.Stat().Persistent(),
		})
		dst.Child().Set(base, f)
	}
	if err != nil {
		return nil, pathError("open", name, err)
	}

	// Reaching here, f is either the existing file opened, or the file
	// newly-created.
	if flag&os.O_TRUNC != 0 {
		if err := f.Truncate(ctx, 0); err != nil {
			return nil, pathError("open", name, err)
		}
	}
	return d.fh(ctx, f, wantWrite), nil
}

// RemoveAll implements part of the webdav.FileSystem interface.
func (d davFS) RemoveAll(ctx context.Context, name string) (err error) {
	defer d.logPrintf("[fs=%v] removeall %q", d, name)(&err)
	if !d.writable {
		return errReadOnly("remove")
	} else if name == "/" {
		return pathError("remove", name, os.ErrPermission)
	}

	clean := cleanPath(name)
	if err := fpath.Remove(ctx, d.pi.File, clean); errors.Is(err, file.ErrChildNotFound) {
		return nil
	} else if err != nil {
		return pathError("remove", clean, err)
	}
	return nil
}

// Rename implements part of the [webdav.FileSystem] interface.
func (d davFS) Rename(ctx context.Context, oldName, newName string) (err error) {
	defer d.logPrintf("[fs=%v] rename %q %q", d, oldName, newName)(&err)
	if !d.writable || oldName == "/" {
		return pathError("rename", oldName, os.ErrPermission)
	} else if newName == "/" {
		return pathError("rename", newName, os.ErrPermission)
	}

	src, err := fpath.OpenPath(ctx, d.pi.File, cleanPath(oldName))
	if err != nil {
		return pathError("rename", oldName, err)
	}
	dir, base := splitPath(newName)
	ddir, err := fpath.Open(ctx, d.pi.File, dir)
	if err != nil {
		return pathError("rename", dir, err)
	}
	if dst, err := ddir.Open(ctx, base); err == nil {
		if dst.Stat().Mode.IsDir() {
			return pathError("rename", base, errors.New("target is a directory"))
		} else if src[len(src)-1].Stat().Mode.IsDir() {
			return pathError("rename", base, errors.New("cannot overwrite target with a directory"))
		}
	} else if !errors.Is(err, file.ErrChildNotFound) {
		return pathError("rename", base, err)
	}

	// Unlink the old file first, as we may be renaming within the same
	// directory.
	if len(src) > 1 {
		src[len(src)-2].Child().Remove(src[len(src)-1].Name())
	}
	ddir.Child().Set(base, src[len(src)-1])
	return nil
}

// Stat implements part of the [webdav.FileSystem] interface.
func (d davFS) Stat(ctx context.Context, name string) (_ os.FileInfo, err error) {
	defer d.logPrintf("[fs=%v] stat %q", d, name)(&err)
	if name == "/" {
		return d.pi.File.FileInfo(), nil
	}

	f, err := fpath.Open(ctx, d.pi.File, cleanPath(name))
	if err != nil {
		return nil, pathError("stat", name, err)
	}
	return f.FileInfo(), nil
}

type davFile struct {
	ctx context.Context // for io APIs
	pi  *filetree.PathInfo

	*file.Cursor
	writable bool
}

// Readdir implements part of the [webdav.File] interface.
// Note this is a different signature than [fs.ReadDirFile].
func (d davFile) Readdir(n int) ([]fs.FileInfo, error) {
	des, err := d.Cursor.ReadDir(n)
	if err != nil {
		return nil, err
	}
	out := make([]fs.FileInfo, len(des))
	for i, de := range des {
		fi, err := de.Info()
		if err != nil {
			return nil, err
		}
		out[i] = fi
	}
	return out, nil
}

// Write implements part of the [webdav.File] interface.
func (d davFile) Write(data []byte) (int, error) {
	if !d.writable {
		return 0, errReadOnly("write")
	}
	return d.Cursor.Write(data)
}

func cleanPath(s string) string {
	return strings.TrimPrefix(path.Clean(s), "/")
}

func splitPath(s string) (dir, base string) {
	clean := path.Clean(s)
	if clean == "/" {
		return
	}
	dir = path.Dir(clean)
	base = path.Base(clean)
	return
}

func pathError(op, path string, err error) error {
	if errors.Is(err, file.ErrChildNotFound) {
		err = os.ErrNotExist
	}
	return &fs.PathError{Op: op, Path: path, Err: err}
}

func errReadOnly(op string) error { return pathError(op, "", os.ErrPermission) }

// This wrapper is a workaround for https://github.com/golang/go/issues/66059.
//
// The default for the WebDAV Overwrite header is supposed to be "T" (true),
// but the Go server implementation treats a missing Overwrite header as "F".
// Rather than fork the library, just patch it on the way in.
type fixOverwriteHandler struct {
	http.Handler
}

func (h fixOverwriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const owHeader = "Overwrite"

	if r.Method == "MOVE" && r.Header.Get(owHeader) == "" {
		r.Header.Set(owHeader, "T")
	}
	h.Handler.ServeHTTP(w, r)
}
