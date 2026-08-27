// Package fscklib implements integrity checks on [file.File] trees.
package fscklib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/mds/mapset"
	"github.com/creachadair/mds/value"
)

// Config carries the configuration for a file check operation.
type Config struct {
	Store           filetree.Store // the store to read from (required)
	ComputeDataSize bool           // whether to compute total data size (expensive)
	Progress        io.Writer      // write progress updates here (nil to discard)
	Errors          io.Writer      // write error diagnostics here (nil means stderr)
}

// Check checks the integrity of the file tree described by origin.  The origin
// must be a format recognized by [filetree.OpenPath]. On success the [Result]
// gives aggregate statistics. An error is only reported if the origin itself
// could not be accessed. The caller should examine the [Result.NumErrors] and
// [Result.NumLost] fields to verify integrity.
func (c Config) Check(ctx context.Context, origin string) (r Result, _ error) {
	of, err := c.Store.OpenPath(ctx, origin)
	if err != nil {
		return r, err
	}
	if of.Root == nil && of.Base == of.File {
		r.Origin = filetree.FormatKey32(of.FileKey)
		c.pprintf("check %s\n", r.Origin)
	} else {
		r.Path = of.Path
		r.Origin = filetree.FormatKey32(of.File.Key())
		c.pprintf("check %q %s\n", r.Path, r.Origin)
	}
	start := time.Now()
	dataSize := make(map[string]int64)
	var done, uniq, lost mapset.Set[string]

	// If this file came from a root pointer, and the root has an index,
	// verify that we can load the index data successfully.
	//
	// If we do have an index, we will also verify that all the reachable
	// file and data blobs are recorded there.
	checkIndex := func(string) bool { return true } // fail open
	if of.Root == nil {
		// no root is invoolved
	} else if of.Root.IndexKey == "" {
		c.eprintf("- root %q is not indexed (OK)\n", of.RootKey)
	} else if idx, err := c.Store.LoadIndex(ctx, of.Root.IndexKey); err != nil {
		c.eprintf("* index %s: %v\n", filetree.FormatKey32(of.Root.IndexKey), err)
		lost.Add(of.Root.IndexKey)
		r.NumErrors++
	} else {
		st := idx.Stats()
		r.Index.Key = filetree.FormatKey32(of.Root.IndexKey)
		r.Index.NumKeys = st.NumKeys
		r.Index.FilterBits = st.FilterBits
		r.Index.NumHashes = st.NumHashes
		c.pprintf("▷ index %s OK (%d keys, %d bits, %d hashes)\n", r.Index.Key,
			r.Index.NumKeys, r.Index.FilterBits, r.Index.NumHashes)
		uniq.Add(of.Root.IndexKey) // lives in the content-addressed store
		r.TotalData++
		checkIndex = idx.Has
	}

	// Verify that all reachable files are loadable, and that their data
	// blocks exist in the store (without fetching them).
	if err := fpath.Walk(ctx, of.File, func(e fpath.Entry) error {
		if e.Err != nil {
			msg := e.Err.Error()
			if e, ok := errors.AsType[*blob.KeyError](e.Err); ok {
				msg = filetree.FormatKey32(e.Key)
				lost.Add(e.Key)
			}
			c.eprintf("* file missing %s %q\n", msg, e.Path)
			r.NumErrors++
			return fpath.ErrSkipChildren // continue scanning
		}

		fkey := e.File.Key()
		if !checkIndex(fkey) {
			c.eprintf("* index: missing file %s\n", filetree.FormatKey32(fkey))
			r.NumErrors++
		}

		// Count each occurrence of a file and its data blocks even if we've already seen it.
		r.TotalFiles++
		fd := e.File.Data()

		want := mapset.New(fd.Keys()...)
		uniq.AddAll(want)
		r.TotalData += fd.Len()

		// If we are asked to compute data size, read each data blob we
		// have not seen before and cache its size.
		if c.ComputeDataSize {
			want.RemoveAll(lost) // exclude keys already known to be lost (we already reported them)
			for dk := range want {
				sz, ok := dataSize[dk]
				if !ok {
					bits, err := c.Store.Files().Get(ctx, dk)
					if errors.Is(err, context.Canceled) {
						return err
					} else if err != nil {
						c.eprintf("* data missing %s %q\n", filetree.FormatKey32(dk), e.Path)
						lost.Add(dk)
						continue
					}
					sz = int64(len(bits))
					dataSize[dk] = sz
				}
				r.TotalDataBytes += sz
			}
		}

		// If (and only if) this is the first time we've seen this file,
		// make sure its data blocks are stored.
		if done.Has(e.File.Key()) {
			return nil // data blocks already checked
		}

		done.Add(fkey)
		for dk := range want {
			if !checkIndex(dk) {
				c.eprintf("* index: missing data %s\n", filetree.FormatKey32(dk))
				r.NumErrors++
			}
		}

		// Check that all the data block keys are at least nominally
		// present in the store.  We don't need to do this if --data-size
		// is set, however, because we've already fetched them all in that
		// case in order to determine their size.
		if !c.ComputeDataSize {
			have, err := c.Store.Files().Has(ctx, fd.Keys()...)
			if err != nil {
				c.eprintf("* check data %q: %v", e.Path, err)
				r.NumErrors++
				return nil
			}
			want.RemoveAll(have)
			if !want.IsEmpty() {
				for m := range want {
					c.eprintf("* data missing %s %q\n", filetree.FormatKey32(m), e.Path)
					lost.Add(m)
				}
			}
		}
		return nil
	}); err != nil {
		return r, err
	}
	for _, v := range dataSize {
		r.UniqueDataBytes += v
	}
	r.TotalObjects = done.Len() + uniq.Len() // N.B. unique includes the index, if there was one
	if c.ComputeDataSize {
		c.pprintf("▷ total data size: %s, %s (%.1f%%)\n",
			formatBytes(r.TotalDataBytes, "bytes"), formatBytes(r.UniqueDataBytes, "unique"),
			percent(r.UniqueDataBytes, r.TotalDataBytes))
	}
	r.UniqueFiles = done.Len()
	r.UniqueData = uniq.Len()
	r.NumLost = lost.Len()
	r.Elapsed = time.Since(start)
	c.pprintf("%s: %d objects: %d files (%d unique, %.1f%%), %d blocks (%d unique, %.1f%%), "+
		"%d lost, %d errors\n",
		value.Cond(r.NumErrors == 0 && r.NumLost == 0, "✅ OK", "❌ FAILED"),
		r.TotalObjects, r.TotalFiles, r.UniqueFiles, percent(r.UniqueFiles, r.TotalFiles),
		r.TotalData, r.UniqueData, percent(r.UniqueData, r.TotalData), r.NumLost, r.NumErrors)
	c.pprintf("🕖 %v elapsed\n", r.Elapsed.Round(time.Millisecond))
	return r, nil
}

// Result is the result of a successful [Config.Check] operation.  Note that a
// check may succeed even if there are integrity errors.
type Result struct {
	Path   string `json:"path,omitzero"` // the path from which this was generated
	Origin string `json:"origin"`        // the storage key of the file tree

	Index struct {
		Key        string `json:"key,omitzero"`       // the storage key of the index, if set
		NumKeys    int    `json:"numKeys,omitzero"`   // number of keys recorded in index
		FilterBits int    `json:"numBits,omitzero"`   // number of filter bits in index
		NumHashes  int    `json:"numHashes,omitzero"` // number of hashes in index
	} `json:"index,omitzero"`

	TotalObjects    int   `json:"objects"`         // total number of objects
	TotalFiles      int   `json:"files"`           // total number of files
	UniqueFiles     int   `json:"uniqueFiles"`     // number of distinct files
	TotalData       int   `json:"data"`            // total number of data blocks
	UniqueData      int   `json:"uniqueData"`      // number of distinct data blocks
	TotalDataBytes  int64 `json:"dataBytes"`       // total number of data bytes
	UniqueDataBytes int64 `json:"uniqueDataBytes"` // total number of unique data bytes

	NumErrors int           `json:"errors,omitzero"` // number of errors
	NumLost   int           `json:"lost,omitzero"`   // number of lost objects
	Elapsed   time.Duration `json:"elapsed"`         // total time elapsed
}

func (c Config) pprintf(msg string, args ...any) { fmtPrintf(c.Progress, msg, args...) }

func (c Config) eprintf(msg string, args ...any) {
	if c.Errors == nil {
		fmtPrintf(os.Stderr, msg, args...)
	}
	fmtPrintf(c.Errors, msg, args...)
}

func fmtPrintf(w io.Writer, msg string, args ...any) {
	if w == nil {
		return
	}
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	fmt.Fprintf(w, msg, args...)
}

func formatBytes(n int64, label string) string {
	if n < 1<<10 {
		return fmt.Sprintf("%d %s", n, label)
	}
	const unit = "KMGTPEZY" // lol
	i, fn := -1, float64(n)
	for fn > 1024 {
		fn /= 1024
		i++
	}
	return fmt.Sprintf("%d %s [%.1f%siB]", n, label, fn, unit[i:i+1])
}

func percent[N ~int | ~int64](v, total N) float64 { return 100 * (float64(v) / float64(total)) }
