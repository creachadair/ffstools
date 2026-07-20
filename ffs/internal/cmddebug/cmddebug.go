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

// Package cmddebug implements the "ffs debug" subcommand family.
package cmddebug

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"iter"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/cache"
	"github.com/creachadair/mds/value"
	"github.com/creachadair/taskgroup"
)

var Command = &command.C{
	Name:     "debug",
	Help:     "Commands for low-level debugging.",
	Unlisted: true,

	Commands: []*command.C{
		{
			Name: "rewrite",
			Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
			Help: `Rewrite file trees into a new store.

Recursively copy the specified file trees into another store.
Unlike sync, which copies the blobs as-written, rewrite completely
regenerates the blobs.

If an argument is a root pointer with no subordinate path, the
root is also copied to the target.`,

			SetFlags: command.Flags(flax.MustBind, &rewriteFlags),
			Run:      command.Adapt(runRewrite),
		},
		{
			Name:     "file-hash",
			Usage:    `path ...`,
			Help:     "Compute the content hash of files in the local filesystem.",
			SetFlags: command.Flags(flax.MustBind, &fileHashFlags),
			Run:      command.Adapt(runFileHash),
		},
		{
			Name:  "file-split",
			Usage: "path ...",
			Help:  "Show the splits of the specified file content into blocks.",
			Run:   command.Adapt(runFileSplit),
		},
		{
			Name:  "key",
			Usage: "<storage-key>...",
			Help: `Convert storage keys into the specified format.

In addition to the standard key formats (see "help key-format"), you may specify
a literal (raw) string prefixed with "@", e.g.,

   @foo     encodes "foo"   (raw)
   @@foo    encodes "@foo"  (raw)

The --to flag specifies the target format to which each key is converted.
THe results are written to stdout.`,
			SetFlags: command.Flags(flax.MustBind, &keyFlags),
			Run:      command.Adapt(runKey),
		},
		{
			Name:     "show-object",
			Usage:    "<storage-key>...",
			Help:     `Render a view the specified objects as JSON.`,
			SetFlags: command.Flags(flax.MustBind, &showObjectFlags),
			Run:      command.Adapt(runShowObject),
		},
		command.InfoCommand("command-info"),
	},
}

var rewriteFlags struct {
	Target string `flag:"to,Target store (required unless --from is set)"`
	Source string `flag:"from,Source store (required unless --to is set)"`
}

func runRewrite(env *command.Env, sourceKeys ...string) error {
	if (rewriteFlags.Target == "") == (rewriteFlags.Source == "") {
		return env.Usagef("exactly one of --from and --to must be set")
	}

	cfg := env.Config.(*config.Settings)
	otherSpec := cmp.Or(rewriteFlags.Target, rewriteFlags.Source)
	return cfg.WithStore(env.Context(), func(main filetree.Store) error {
		return cfg.WithStoreAddress(env.Context(), otherSpec, func(other filetree.Store) error {
			var src, tgt filetree.Store
			if rewriteFlags.Target != "" {
				fmt.Fprintf(env, "Target store: %q\n", rewriteFlags.Target)
				src, tgt = main, other
			} else {
				fmt.Fprintf(env, "Source store: %q\n", rewriteFlags.Source)
				src, tgt = other, main
			}

			for _, arg := range sourceKeys {
				pi, err := src.OpenPath(env.Context(), arg)
				if err != nil {
					return err
				}

				fmt.Fprintf(env, "Rewriting %q...\n", arg)
				c := cache.New(cache.LRU[string, *file.File]().WithLimit(1 << 20))
				rf, err := rewriteRecursive(env.Context(), pi.File, tgt, c)
				if err != nil {
					return err
				}
				rfKey, err := rf.Flush(env.Context())
				if err != nil {
					return err
				}

				if pi.Root != nil && pi.File == pi.Base {
					if pi.Root.IndexKey != "" {
						fmt.Fprintf(env, "WARNING: Root index %s not copied, it must be re-generated\n",
							filetree.FormatKey32(pi.Root.IndexKey))
					}
					nr := root.New(tgt.Roots(), &root.Options{
						FileKey:     rf.Key(),
						Description: pi.Root.Description,
					})
					fmt.Fprintf(env, "Copying root %q...\n", pi.RootKey)
					if err := nr.Save(env.Context(), pi.RootKey); err != nil {
						return fmt.Errorf("save root: %w", err)
					}
				}
				fmt.Printf("src: %s\n", filetree.FormatKey32(pi.File.Key()))
				fmt.Printf("dst: %s\n", filetree.FormatKey32(rfKey))
			}
			return nil
		})
	})
}

func rewriteRecursive(ctx context.Context, f *file.File, tgt filetree.Store, seen *cache.Cache[string, *file.File]) (*file.File, error) {
	if sf, ok := seen.Get(f.Key()); ok {
		return sf, nil
	}
	nf := file.New(tgt.Files(), &file.NewOptions{
		Name:        f.Name(),
		Stat:        value.Ptr(f.Stat()),
		PersistStat: f.Stat().Persistent(),
	})
	var g taskgroup.Group
	for _, kid := range f.Child().Names() {
		kf, err := f.Open(ctx, kid)
		if err != nil {
			return nil, fmt.Errorf("open child: %w", err)
		}
		if kf.Child().Len() == 0 {
			g.Go(func() error {
				rkf, err := rewriteRecursive(ctx, kf, tgt, seen)
				if err != nil {
					return err
				}
				nf.Child().Set(kid, rkf)
				return nil
			})
		} else if rkf, err := rewriteRecursive(ctx, kf, tgt, seen); err != nil {
			g.Wait()
			return nil, err
		} else {
			nf.Child().Set(kid, rkf)
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if d := f.Data(); d.Size() > 0 {
		if err := nf.SetData(ctx, f.Cursor(ctx)); err != nil {
			return nil, fmt.Errorf("copy data: %w", err)
		}
	}
	if xa := f.XAttr(); xa.Len() > 0 {
		nxa := nf.XAttr()
		for _, name := range xa.Names() {
			nxa.Set(name, xa.Get(name))
		}
	}

	// Note: Updating the children touches the file, so reset the modtime.
	nf.Stat().WithModTime(f.Stat().ModTime).Update()
	seen.Put(f.Key(), nf) // N.B. original file key, not the new one
	return nf, nil
}

var fileHashFlags struct {
	Verbose bool `flag:"v,Print verbose statistics"`
}

func runFileHash(env *command.Env, paths ...string) error {
	kv := memstore.NewKV()
	for _, path := range paths {
		kv.Clear()
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		cas := blob.CASFromKV(kv)
		cf := file.New(cas, nil)
		err = cf.SetData(env.Context(), f)
		f.Close()
		if err != nil {
			return fmt.Errorf("set data: %w", err)
		}
		h := cf.Data().Hash()
		if len(paths) != 1 {
			fmt.Print(path, "\t")
		}
		fmt.Println(filetree.FormatKey64(string(h)))
		if fileHashFlags.Verbose {
			all := kv.Snapshot(nil)
			var totalBytes int
			for _, v := range all {
				totalBytes += len(v)
			}
			fmt.Fprintf(env, "- blocks: %d, total bytes: %d\n", len(all), totalBytes)
		}
	}
	return nil
}

func runFileSplit(env *command.Env, paths ...string) error {
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		if len(paths) > 1 {
			fmt.Println("#", path)
		}
		cas := blob.CASFromKV(new(logKV))
		cf := file.New(cas, nil)
		err = cf.SetData(env.Context(), f)
		f.Close()
		if err != nil {
			return fmt.Errorf("split data: %w", err)
		}
	}
	return nil
}

type logKV struct{ pos int64 }

func (*logKV) Get(context.Context, string) ([]byte, error)         { return nil, blob.ErrKeyNotFound }
func (*logKV) Has(context.Context, ...string) (blob.KeySet, error) { return nil, nil }
func (*logKV) Delete(context.Context, string) error                { return blob.ErrKeyNotFound }
func (*logKV) Len(context.Context) (int64, error)                  { return 0, nil }

func (*logKV) List(context.Context, string) iter.Seq2[string, error] {
	return func(_ func(string, error) bool) {}
}

func (kv *logKV) Put(_ context.Context, opts blob.PutOptions) error {
	n := int64(len(opts.Data))
	fmt.Printf("%d %d %s\n", kv.pos, n, filetree.FormatKey64(opts.Key))
	kv.pos += n
	return nil
}

var showObjectFlags struct {
	Root bool `flag:"root,View objects in the root KV"`
}

func runShowObject(env *command.Env, storageKeys ...string) error {
	if len(storageKeys) == 0 {
		return env.Usagef("no storage keys provided")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(src filetree.Store) error {
		var kv blob.KV
		if showObjectFlags.Root {
			kv = src.Roots()
		} else {
			kv = src.Sync()
		}
		for _, sk := range storageKeys {
			var key string
			if showObjectFlags.Root {
				key = sk
			} else if pk, err := filetree.ParseKey(sk); err != nil {
				return err
			} else {
				key = pk
			}

			var obj wiretype.Object
			if err := wiretype.Load(env.Context(), kv, key, &obj); err != nil {
				data, err := kv.Get(env.Context(), key)
				if err != nil {
					return err
				}
				fmt.Println(config.ToJSON(map[string]any{
					"storageKey": filetree.FormatKey32(key),
					"data":       data,
				}))
				return nil
			}

			out := map[string]any{
				"storageKey": filetree.FormatKey32(key),
			}
			switch t := obj.Value.(type) {
			case *wiretype.Object_Node:
				out["node"] = t.Node
			case *wiretype.Object_Root:
				out["root"] = t.Root
			case *wiretype.Object_Index:
				out["index"] = t.Index
			default:
				return fmt.Errorf("unknown object type %T for %q", obj.Value, filetree.FormatKey32(key))
			}
			fmt.Println(config.ToJSON(out))
		}
		return nil
	})
}

var keyFlags struct {
	To string `flag:"to,default=32,Convert keys to this format (raw, hex, 32, 64)"`
}

func runKey(env *command.Env, keys ...string) error {
	var formatKey func(string) string
	switch strings.ToLower(keyFlags.To) {
	case "hex", "16", "key16", "k16":
		formatKey = func(key string) string { return hex.EncodeToString([]byte(key)) }
	case "32", "k32", "key32", "b32", "base32":
		formatKey = filetree.FormatKey32
	case "64", "k64", "key64", "b64", "base64":
		formatKey = filetree.FormatKey64
	case "raw":
		formatKey = func(s string) string { return s }
	default:
		return fmt.Errorf("unknown key format %q", keyFlags.To)
	}
	for i, key := range keys {
		parsed, err := filetree.ParseKey(key)
		if err != nil {
			return fmt.Errorf("key %d: %w", i+1, err)
		}
		out := formatKey(parsed)
		fmt.Print(out)
		if utf8.ValidString(out) {
			fmt.Println()
		}
	}
	return nil
}
