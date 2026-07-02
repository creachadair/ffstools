// Package exportlib defines common plumbing for exporting file trees
// into archives and other filesystems.
package exportlib

import (
	"archive/tar"
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/mds/value"
)

var dirStat = &file.Stat{Mode: fs.ModeDir | 0755}

// Config carries shared settings for exporting file trees from a store.
// A zero value is ready for use.
type Config struct {
	Root         string    // prefix output paths with this directory
	IncludeXAttr bool      // include extended attributes, if possible
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

// FileToZIP recursively exports the complete contents of tree into zw.
func (c Config) FileToZIP(ctx context.Context, tree *filetree.PathInfo, zw *zip.Writer) error {
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

// FileToTar recursively exports the contents of tree to tw.
func (c Config) FileToTar(ctx context.Context, tree *filetree.PathInfo, tw *tar.Writer) error {
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
