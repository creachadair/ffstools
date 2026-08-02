// Copyright 2022 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package synclib implements synchronization of file trees between stores.
//
// # Usage
//
// Construct a [Config] with Source and Target stores populated:
//
//	sc := synclib.Config{Source: src, Target: tgt}
//
// Call [Config.Sync] to copy file trees from Source to Target:
//
//	stats, err := sc.Sync(ctx, []string{"foo", "bar/baz", "quux"})
package synclib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/lib/pbar"
	"github.com/creachadair/taskgroup"
)

type Config struct {
	Source filetree.Store // required
	Target filetree.Store // required

	// If true, any root pointer being copied whole must have a cached index, or
	// Sync will report an error.
	RequireIndex bool

	// If true, ignore cached indexes when copying root pointers
	NoIndex bool

	// If true, do not copy root pointers, only sync their contents.
	SkipRoots bool

	// If set, this function is called with the storage key of each root pointer
	// to be copied, and its return value (if non-empty) replaces the original name.
	// Otherwise, roots retain their old names in the target.
	RenameRoot func(string) string

	// If set, report progress messages to this function.
	// Only message types covered by ProgressLevel will be sent.
	Progress ProgressFunc

	// What types of progress message will be sent if Progress is non-nil.
	// Zero means ProgressInfo. To omit all progress logs, set Progress = nil.
	ProgressLevel int

	// If non-nil, write a progress bar to this writer when appropriate.
	BarWriter io.Writer
}

// Stats report statistics about a sync process.
type Stats struct {
	Copied  int64
	Elapsed time.Duration
}

// Sync synchronizes the file trees described by sourceKeys from c.Source to
// c.Target.  It copies all and only those objects recursively reachable, that
// are not already present in the target.
//
// Each source key specifies the name of a root pointer, the content address of
// a file tree, or a path descended from one or the other of these.
//
// For example:
//
//	files.backup
//	files.backup/notes/laundry-list.txt
//	@etv2mjn8wuf263d4xyegkveky1
//	@etv2mjn8wuf263d4xyegkveky1/code/.git
//
// For a source key naming a root pointer, there are some special behaviors:
//
//   - By default, the root itself is copied along with the file tree.
//     Set [Config.SkipRoots] to skip copying the root itself.
//
//   - If the root has an index, it is used to find the objects to copy.
//     Set [Config.NoIndex] to ignore the index and re-scan the root.
//
//   - When copying a root, the name is preserved in the target. To use a
//     different name, set [Config.RenameRoot].
func (c Config) Sync(ctx context.Context, sourceKeys []string) (Stats, error) {
	if len(sourceKeys) == 0 {
		return Stats{}, nil // nothing to do
	} else if !c.Source.IsValid() || !c.Target.IsValid() {
		return Stats{}, errors.New("both Source and Target are required")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, run := taskgroup.New(cancel).Limit(64)

	// Find all the objects reachable from the specified starting points.
	// For an indexed root, capture the index and scan the whole corpus.
	// Otherwise, scan from the target file and copy any missing items.
	var stats Stats
	syncStart := time.Now()
	var indices []*index.Index
	var roots []*filetree.PathInfo

	for _, elt := range sourceKeys {
		of, err := c.Source.OpenPath(ctx, elt)
		if err != nil {
			return stats, err
		}

		scanStart := time.Now()
		if of.Root != nil && of.Base == of.File {
			roots = append(roots, of) // to be copied, below
			if c.RequireIndex && of.Root.IndexKey == "" {
				return stats, fmt.Errorf("missing required index for %q", elt)
			} else if of.Root.IndexKey != "" && !c.NoIndex {
				idx, err := c.Source.LoadIndex(ctx, of.Root.IndexKey)
				if err != nil {
					return stats, err
				}
				c.detailf("Loaded cached index for %q (%d keys)\n", elt, idx.Len())
				indices = append(indices, idx)
				continue
			}
			c.infof("Scanning data reachable from root %q ...\n", of.RootKey)
		} else {
			c.infof("Scanning data reachable from file %s ...\n", filetree.FormatKey32(of.FileKey))
		}
		nk, err := c.scanAndCopy(ctx, of.File, run)
		c.infof("Scan complete [%d copied, %v elapsed]\n", nk, time.Since(scanStart).Round(time.Millisecond))
		stats.Copied += int64(nk)
		if err != nil {
			return stats, err
		}
	}
	if err := g.Wait(); err != nil {
		return stats, err
	}

	// If we loaded cached indices, copy any missing keys.
	if len(indices) != 0 {
		missing, err := findMissing(ctx, indices, c.Source.Files(), c.Target.Sync())
		if err != nil {
			return stats, err
		}
		c.detailf("Key scan found %d missing keys\n", len(missing))
		var pb *pbar.Bar
		if len(missing) > 1000 && c.BarWriter != nil {
			pb = pbar.New(c.BarWriter, int64(len(missing))).Start()
		}
		for key := range missing {
			if ctx.Err() != nil {
				break
			}
			run(func() error {
				pb.Add(1)
				defer pb.AddMeta(1)
				return copyBlob(ctx, c.Source.Sync(), c.Target.Sync(), key, false)
			})
		}
		if err := g.Wait(); err != nil {
			return stats, err
		}
		pb.Stop()
		c.detailf("Copied %d reachable objects from %d indices\n", len(missing), len(indices))
		stats.Copied += int64(len(missing))
	}

	// Copy any roots (and associated index blobs) that were mentioned as starting points.
	for _, root := range roots {
		key := root.RootKey
		if c.SkipRoots {
			c.detailf("NOTE: Skipping root %q\n", key)
			continue
		}
		c.debugf("- copying root %q", key)
		if ik := root.Root.IndexKey; ik != "" {
			// Do the index first, so we don't copy a broken root pointer if it fails.
			if err := copyBlob(ctx, c.Source.Files(), c.Target.Sync(), ik, false); err != nil {
				return stats, err
			}
			stats.Copied++
		}
		tkey := key
		if c.RenameRoot != nil {
			tkey = c.RenameRoot(key)
			if tkey == "" {
				tkey = key
			}
		}
		if err := moveBlob(ctx, c.Source.Roots(), c.Target.Roots(), key, tkey, true); err != nil {
			return stats, err
		}
		stats.Copied++
	}
	stats.Elapsed = time.Since(syncStart)
	return stats, nil
}

func (c Config) infof(msg string, args ...any) {
	if c.Progress != nil && c.ProgressLevel >= 0 {
		c.Progress(ProgressInfo, msg, args...)
	}
}

func (c Config) detailf(msg string, args ...any) {
	if c.Progress != nil && c.ProgressLevel >= ProgressDetail {
		c.Progress(ProgressDetail, msg, args...)
	}
}

func (c Config) debugf(msg string, args ...any) {
	if c.Progress != nil && c.ProgressLevel >= ProgressDebug {
		c.Progress(ProgressDebug, msg, args...)
	}
}

func copyBlob(ctx context.Context, src blob.KVCore, tgt blob.KV, key string, replace bool) error {
	return moveBlob(ctx, src, tgt, key, key, replace)
}

func moveBlob(ctx context.Context, src blob.KVCore, tgt blob.KV, oldKey, newKey string, replace bool) error {
	bits, err := src.Get(ctx, oldKey)
	if err != nil {
		return err
	}
	err = tgt.Put(ctx, blob.PutOptions{
		Key:     newKey,
		Data:    bits,
		Replace: replace,
	})
	if blob.IsKeyExists(err) {
		err = nil
	}
	return err
}

// scanAndCopy scans all the content-addressed blobs reachable from root in
// c.Source, and copies any to c.Target that are not already present there.
//
// Copies are executed concurrently via run.  It reports the total number of
// copies successfully issued, but the caller must wait for the taskgroup to
// settle before reporting success.
//
// Copying during the scan, rather than separately, takes better advantage of
// a store cache, since the file being visited is likely to be still warm when
// we do the copy. This is especially helpful for remote stores, where the cost
// of re-fetching a blob faulted out may be substantial.
func (c Config) scanAndCopy(ctx context.Context, root *file.File, run taskgroup.StartFunc) (int, error) {
	src := c.Source.Files()
	tgt := c.Target.Sync()

	var seen, data blob.KeySet
	var nc int
	var check []string
	pt := time.NewTicker(10 * time.Second)
	defer pt.Stop()
	start := time.Now()
	err := root.Scan(ctx, func(si file.ScanItem) error {
		if ctx.Err() != nil {
			return ctx.Err()
		} else if seen.Has(si.Key()) {
			return file.ErrSkipChildren // already visited
		}
		check = append(check[:0], si.Key())
		for _, dk := range si.Data().Keys() {
			if !data.Has(dk) {
				check = append(check, dk)
				data.Add(dk)
			}
		}
		need, err := blob.SyncKeys(ctx, tgt, check)
		if err != nil {
			return err
		}
		nc += need.Len()

		// Do the file first, while it is still hot in the cache.
		if need.Has(si.Key()) {
			if err := copyBlob(ctx, src, tgt, si.Key(), false); err != nil {
				return err
			}
			need.Remove(si.Key())
			seen.Add(si.Key())
		}
		for missing := range need {
			run(func() error {
				return copyBlob(ctx, src, tgt, missing, false)
			})
		}
		select {
		case <-pt.C:
			c.detailf("... [%v] scan progress: %d files, %d blocks\n",
				time.Since(start).Round(time.Millisecond), len(seen), len(data))
		default:
		}
		return nil
	})
	return nc, err
}

// findMissing reports the set of all keys in src mentioned by one of the
// indices, but not present in tgt.
func findMissing(ctx context.Context, indices []*index.Index, src blob.KVCore, tgt blob.KV) (blob.KeySet, error) {
	var want blob.KeySet
	for key, err := range src.List(ctx, "") {
		if err != nil {
			return nil, err
		}
		for _, idx := range indices {
			if idx.Has(key) {
				want.Add(key)
				break
			}
		}
	}
	for ch := range slices.Chunk(want.Slice(), 64) {
		if ctx.Err() != nil {
			break
		}
		have, err := tgt.Has(ctx, ch...)
		if err != nil {
			return nil, err
		}
		want.RemoveAll(have)
	}
	return want, nil
}

// Progress logging levels.
const (
	ProgressInfo   = 1 + iota // report informational messages
	ProgressDetail            // report detail messages
	ProgressDebug             // report debugging messages
)

// A ProgressFunc is a callback used to report progress during a sync.
// The level argument indicates the detail level of the message.
type ProgressFunc func(level int, msg string, args ...any)
