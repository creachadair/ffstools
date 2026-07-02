// Package exportlib defines common plumbing for exporting file trees
// into archives and other filesystems.
package exportlib

import (
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
)

var dirStat = &file.Stat{Mode: fs.ModeDir | 0755}

// Config carries shared settings for exporting file trees from a store.
// A zero value is ready for use.
type Config struct {
	Root        string    // prefix output paths with this directory
	DebugOutput io.Writer // write detailed debug output here
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
