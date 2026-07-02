// Package exportlib defines common plumbing for exporting file trees
// into archives and other filesystems.
package exportlib

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/mds/value"
	"github.com/creachadair/taskgroup"
	"github.com/pkg/xattr"
)

var dirStat = &file.Stat{Mode: fs.ModeDir | 0755}

// Config carries shared settings for exporting file trees from a store.
// A zero value is ready for use.
type Config struct {
	Root         string    // prefix output paths with this directory
	IncludeXAttr bool      // include extended attributes, if possible
	OmitStat     bool      // omit permissions and modification times
	Update       bool      // update or replace existing targets
	DebugOutput  io.Writer // write detailed debug output here
}

func (c Config) dprintf(msg string, args ...any) {
	if c.DebugOutput != nil {
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fmt.Fprintf(c.DebugOutput, msg, args...)
	}
}

// ExportToZIP recursively exports the contents of tree into zw.
func (c Config) ExportToZIP(ctx context.Context, tree *filetree.PathInfo, zw *zip.Writer) error {
	root := tree.File
	if strings.Contains(tree.Path, "/") || c.Root != "" {
		p := path.Join(c.Root, path.Base(tree.Path))
		root = tree.File.New(&file.NewOptions{Stat: dirStat})
		if _, err := fpath.Set(ctx, root, p, &fpath.SetOptions{
			Create:  true,
			SetStat: func(s *file.Stat) { s.Mode = fs.ModeDir | 0755 },
			File:    tree.File,
		}); err != nil {
			return err
		}
	}
	if werr := c.addFileToZip(ctx, zw, root); werr != nil {
		return fmt.Errorf("copy to archive: %w", werr)
	}
	return nil
}

func (c Config) addFileToZip(ctx context.Context, zw *zip.Writer, root *file.File) error {
	return fpath.Walk(ctx, root, func(e fpath.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		} else if e.Err != nil {
			return e.Err
		} else if e.File == root {
			return nil // skip
		}
		fi := e.File.FileInfo()
		fh, err := zip.FileInfoHeader(fi)
		if err != nil {
			return fmt.Errorf("file info %q: %w", e.Path, err)
		}
		fh.Name = e.Path
		if fi.IsDir() {
			fh.Name += "/"
		}
		fh.Method = zip.Deflate
		h, err := zw.CreateHeader(fh)
		if err != nil {
			return fmt.Errorf("zip header %q: %w", e.Path, err)
		}
		if fi.IsDir() {
			c.dprintf("dir: %s", e.Path)
			return nil
		}
		_, cerr := io.Copy(h, e.File.Cursor(ctx))
		if cerr == nil {
			c.dprintf("+ %s: %s", fileType(fi), e.Path)
		}
		return cerr
	})
}

func fileType(fi fs.FileInfo) string {
	if fi.IsDir() {
		return "dir"
	} else if fi.Mode().IsRegular() {
		return "file"
	} else if fi.Mode()&fs.ModeSymlink != 0 {
		return "link"
	}
	return "other"
}

// ExportToTar recursively exports the contents of tree to tw.
func (c Config) ExportToTar(ctx context.Context, tree *filetree.PathInfo, tw *tar.Writer) error {
	tdir := c.Root
	if strings.Contains(tree.Path, "/") {
		tdir = path.Join(tdir, path.Base(tree.Path))
	}
	root := tree.File

	// This is a demi-clone of [tar.Writer.AddFS], but with less OS-specific nonsense.
	return fpath.Walk(ctx, root, func(e fpath.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		} else if e.Err != nil {
			return e.Err
		} else if e.File == root {
			return nil // skip
		}
		fi := e.File.FileInfo()
		c.dprintf("a %s", path.Join(tdir, e.Path))

		// If this is a symlink, read the "file" contents out as the target.
		var linkTarget string
		if fi.Mode().Type() == fs.ModeSymlink {
			link, err := io.ReadAll(e.File.Cursor(ctx))
			if err != nil {
				return fmt.Errorf("read symlink: %w", err)
			}
			linkTarget = string(link)
			c.dprintf("  link to %q", linkTarget)
		}

		// This does a bunch of nonsense we don't care about, but it handles the
		// ustar-specific encoding of file type bits that would be annoying to copy.
		// We'll adjust some of the results before writing the header, see below.
		h, err := tar.FileInfoHeader(lyingFileInfo{fi}, linkTarget)
		if err != nil {
			return err
		}

		// Replace the base name with the full path, including the prefix (if any).
		h.Name = path.Join(tdir, e.Path)
		if fi.Mode().IsDir() {
			h.Name += "/" // suffix directories with "/"
		}

		// Populate the owner and group IDs, as otherwise they will default to 0
		// and that makes the tar annoying to read when unpacked.
		fs := e.File.Stat()
		h.Uid = fs.OwnerID
		h.Uname = fs.OwnerName
		h.Gid = fs.GroupID
		h.Gname = fs.GroupName

		// If there are extended attributes, and we were asked to preserve them, do.
		if xa := e.File.XAttr(); xa.Len() != 0 && c.IncludeXAttr {
			c.dprintf("  + %d extended attribute%s", xa.Len(), value.Cond(xa.Len() == 1, "", "s"))
			m := make(map[string]string)
			for _, name := range xa.Names() {
				m[name] = xa.Get(name)
			}
			//lint:ignore SA1019 This field is supposedly deprecated, but Go 1 protects us.
			h.Xattrs = m
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			_, err := io.Copy(tw, e.File.Cursor(ctx))
			return err
		}
		return nil
	})
}

// lyingFileInfo pretends to implement the [tar.FileInfoNames] interface so
// that the header constructor won't try to do name lookups on the system.
// But it just reports empty names, since we can fill those ourselves.
type lyingFileInfo struct{ fs.FileInfo }

func (lyingFileInfo) Uname() (string, error) { return "", nil }
func (lyingFileInfo) Gname() (string, error) { return "", nil }

// ExportToOS recursively exports the contents of tree into the specified
// outputPath in the native filesystem.
func (c Config) ExportToOS(ctx context.Context, tree *filetree.PathInfo, outputPath string) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, start := taskgroup.New(cancel).Limit(runtime.NumCPU())

	// We have to update directory timestamps after their contents are
	// unpacked, otherwise the results are overwritten.
	dirs := make(map[string]*file.File)
	g.Go(func() error {
		return fpath.Walk(cctx, tree.File, func(e fpath.Entry) error {
			if err := cctx.Err(); err != nil {
				return err
			}

			opath := filepath.Join(outputPath, filepath.FromSlash(e.Path))
			if fs := e.File.Stat(); !fs.Mode.IsDir() {
				start(func() error {
					return c.exportFile(cctx, e.File, opath)
				})
				return nil
			} else if !c.OmitStat && fs.Persistent() {
				dirs[opath] = e.File
			}
			return c.exportFile(cctx, e.File, opath)
		})
	})
	if err := g.Wait(); err != nil {
		return err
	}
	for path, dir := range dirs {
		start(func() error {
			stat := dir.Stat()
			c.dprintf("+ update dir %q set mtime %v", path, stat.ModTime.Format(time.RFC3339))
			return os.Chtimes(path, stat.ModTime, stat.ModTime)
		})
	}
	return g.Wait()
}

func (c Config) exportFile(ctx context.Context, f *file.File, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mode := f.Stat().Mode
	var link bool
	if mode.IsDir() {
		c.dprintf("create directory %q", path)
		if err := os.Mkdir(path, 0700); err != nil {
			if !c.Update || !os.IsExist(err) {
				return err
			}
		}
	} else if mode.Type()&fs.ModeSymlink != 0 {
		c.dprintf("write symlink %q", path)
		if err := linkFile(ctx, f, path); err != nil {
			return err
		}
		link = true
	} else {
		if !c.Update {
			_, err := os.Lstat(path)
			if err == nil {
				return fmt.Errorf("file %q exists", path)
			}
		}
		nw, err := copyFile(ctx, f, path)
		if err != nil {
			return err
		}
		c.dprintf("write file %q (%d bytes)", path, nw)
	}

	// Restore permissions and modification times, if requested and available.
	if !c.OmitStat && f.Stat().Persistent() && !link {
		stat := f.Stat()
		c.dprintf("- set mode %v, mtime %v", stat.Mode.Perm(), stat.ModTime.Format(time.RFC3339))

		if err := os.Chmod(path, stat.Mode); err != nil {
			return fmt.Errorf("setting permissions: %w", err)
		}
		if !mode.IsDir() {
			if err := os.Chtimes(path, stat.ModTime, stat.ModTime); err != nil {
				return fmt.Errorf("setting modtime: %w", err)
			}
		}
		// TODO(creachadair): Maybe set owner/group?
	}

	// Restore extended attributes if requested.
	if c.IncludeXAttr {
		xa := f.XAttr()
		for _, key := range xa.Names() {
			val := xa.Get(key)
			c.dprintf("- set xattr %q (%d bytes)", key, len(val))
			if xerr := xattr.LSet(path, key, []byte(val)); xerr != nil {
				return fmt.Errorf("setting xattrs %q: %w", key, xerr)
			}
		}
	}
	return nil
}

func copyFile(ctx context.Context, f *file.File, path string) (int64, error) {
	r := bufio.NewReaderSize(f.Cursor(ctx), 1<<20)
	return atomicfile.WriteAll(path, r, 0600)
}

func linkFile(ctx context.Context, f *file.File, path string) error {
	target, err := io.ReadAll(f.Cursor(ctx))
	if err != nil {
		return fmt.Errorf("reading link target: %w", err)
	}
	return os.Symlink(string(target), path)
}
