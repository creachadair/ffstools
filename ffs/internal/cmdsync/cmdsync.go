// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package cmdsync

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
)

var syncFlags struct {
	Target  string
	Verbose bool
}

func debug(msg string, args ...interface{}) {
	if syncFlags.Verbose {
		log.Printf(msg, args...)
	}
}

var Command = &command.C{
	Name: "sync",
	Usage: `<file-key>[/path/...] ...
@<root-key>[/path/...] ...`,
	Help: `Synchronize file trees between stores.

Transfer all the blobs reachable from the specified file or root
paths into the given target store.
`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.StringVar(&syncFlags.Target, "to", "", "Target store (required)")
		fs.BoolVar(&syncFlags.Verbose, "v", false, "Enable verbose logging")
	},
	Run: runSync,
}

func runSync(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing source keys")
	} else if syncFlags.Target == "" {
		return env.Usagef("missing -to target store")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(src blob.CAS) error {
		taddr := cfg.ResolveAddress(syncFlags.Target)
		return config.WithStore(cfg.Context, taddr, func(tgt blob.CAS) error {
			fmt.Fprintf(env, "Target store: %q\n", taddr)

			// Find all the blobs reachable from the specified starting points.
			worklist := make(scanSet)
			for _, elt := range args {
				of, err := config.OpenPath(cfg.Context, src, elt)
				if err != nil {
					return err
				}

				if of.Root != nil && of.Base == of.File {
					fmt.Fprintf(env, "Scanning data reachable from root %q\n", of.RootKey)
					err = worklist.root(cfg.Context, src, of.RootKey, of.Root)
				} else {
					fmt.Fprintf(env, "Scanning data reachable from file %x\n", of.FileKey)
					err = worklist.file(cfg.Context, of.File)
				}
				if err != nil {
					return err
				}
			}
			fmt.Fprintf(env, "Found %d reachable objects\n", len(worklist))
			if len(worklist) == 0 {
				return errors.New("no matching objects")
			}

			// Remove from the worklist all blobs already stored in the target
			// that are not scheduled for replacement. Blobs marked as root (R) or
			// otherwise requiring replacement (+) are retained regardless.
			if err := tgt.List(cfg.Context, "", func(key string) error {
				switch worklist[key] {
				case '-', 'F':
					delete(worklist, key)
				}
				return nil
			}); err != nil {
				return err
			}
			fmt.Fprintf(env, "Have %d objects to copy\n", len(worklist))

			// Copy all remaining objects.
			start := time.Now()
			var nb int64

			ctx, cancel := context.WithCancel(cfg.Context)
			defer cancel()

			g, run := taskgroup.New(taskgroup.Trigger(cancel)).Limit(128)
			for key, tag := range worklist {
				if ctx.Err() != nil {
					break
				}

				key, tag := key, tag
				run(func() error {
					defer atomic.AddInt64(&nb, 1)
					switch tag {
					case 'R':
						debug("- copying root %q", key)
						return copyBlob(ctx, config.Roots(src), config.Roots(tgt), key, true)
					case '+':
						return copyBlob(ctx, src, tgt, key, true)
					case 'F':
						debug("- copying file %x", key)
						return copyBlob(ctx, src, tgt, key, false)
					case '-':
						return copyBlob(ctx, src, tgt, key, false)
					default:
						panic("unknown tag " + string(tag))
					}
				})
			}
			cerr := g.Wait()
			fmt.Fprintf(env, "Copied %d blobs [%v elapsed]\n",
				nb, time.Since(start).Truncate(10*time.Millisecond))
			return cerr
		})
	})
}

type scanSet map[string]byte

func (s scanSet) root(ctx context.Context, src blob.CAS, rootKey string, rp *root.Root) error {
	s[rootKey] = 'R'
	s[rp.OwnerKey] = '+'
	s[rp.IndexKey] = '-'
	if rp.Predecessor != "" {
		proot, err := root.Open(ctx, config.Roots(src), rp.Predecessor)
		if err != nil {
			return err
		} else if err := s.root(ctx, src, rp.Predecessor, proot); err != nil {
			return err
		}
	}
	fp, err := rp.File(ctx, src)
	if err != nil {
		return err
	}
	return s.file(ctx, fp)
}

func (s scanSet) file(ctx context.Context, fp *file.File) error {
	return fp.Scan(ctx, func(key string, isFile bool) bool {
		if _, ok := s[key]; ok {
			return false
		} else if isFile {
			s[key] = 'F'
		} else {
			s[key] = '-'
		}
		return true
	})
}

func copyBlob(ctx context.Context, src, tgt blob.CAS, key string, replace bool) error {
	if key == "" {
		return nil
	}
	bits, err := src.Get(ctx, key)
	if err != nil {
		return err
	}
	err = tgt.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    bits,
		Replace: replace,
	})
	if blob.IsKeyExists(err) {
		err = nil
	}
	return err
}
