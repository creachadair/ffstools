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

// Package cmdroot implements the "ffs root" subcommand.
package cmdroot

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"text/tabwriter"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/putlib"
	"github.com/creachadair/flax"
)

var Command = &command.C{
	Name: "root",
	Help: "Manipulate filesystem root pointers.",

	Commands: []*command.C{
		{
			Name:  "list",
			Usage: "[name-glob ...]",
			Help: `List the root keys known in the store.

If name globs are provided, only names matching those globs are listed; otherwise
all known keys are listed.`,

			SetFlags: command.Flags(flax.MustBind, &listFlags),
			Run:      runList,
		},
		{
			Name:  "create",
			Usage: "<name>\n<name> <file-key>\n--ref <name> <root>/<path>",
			Help: `Create a root pointer.

If only a <name> is given, a new empty root pointer is created with that name.
If a <file-key> is specified, the new root points to that file (which must exist).

With --ref, the specified root/path is resolved, and root points to it.

With --put, the specified filesystem path is copied, and root points to it.
This copy is performed with default settings. For full control over the copy,
use the "put" command separately.`,

			SetFlags: command.Flags(flax.MustBind, &createFlags),
			Run:      command.Adapt(runCreate),
		},
		{
			Name:  "copy",
			Usage: "<source-name> <target-name>",
			Help:  "Duplicate a root pointer under a new name.",

			SetFlags: command.Flags(flax.MustBind, &copyFlags),
			Run:      command.Adapt(runCopy),
		},
		{
			Name:  "rename",
			Usage: "<source-name> <target-name>",
			Help:  "Rename a root pointer (equivalent to copy + remove).",

			SetFlags: command.Flags(flax.MustBind, &copyFlags),
			Run:      command.Adapt(runCopy),
		},
		{
			Name:  "delete",
			Usage: "<root-key> ...",
			Help:  "Delete the specified root pointers.",

			Run: runDelete,
		},
		{
			Name:  "set-desc",
			Usage: "<name> <description>...",
			Help:  "Edit the description of the given root pointer.",

			Run: command.Adapt(runEditDesc),
		},
		{
			Name:  "set-file",
			Usage: "<name> <file-key>",
			Help: `Edit the file key of the given root.

If a <file-key> is specified, it must already exist in the store.`,

			Run: command.Adapt(runEditFile),
		},
		{
			Name:  "index",
			Usage: "<root-key> ...",
			Help: `
Update each of the specified roots to include a blob index.

An index is a Bloom filter of the keys reachable from the root.  If a root
already has an index, it is not changed; use -f to force a new index to be
computed anyway.`,

			SetFlags: command.Flags(flax.MustBind, &indexFlags),
			Run:      runIndex,
		},
	},
}

var listFlags struct {
	Long bool `flag:"long,Print details for each root"`
	JSON bool `flag:"json,Format output as JSON"`
}

func matchAny(key string, globs []string) bool {
	for _, glob := range globs {
		if ok, _ := path.Match(glob, key); ok {
			return true
		}
	}
	return len(globs) == 0
}

func runList(env *command.Env) error {
	glob := env.Args
	if len(glob) == 0 {
		glob = append(glob, "*")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		w := tabwriter.NewWriter(os.Stdout, 4, 2, 1, ' ', 0)
		defer w.Flush()

		for key, err := range s.Roots().List(env.Context(), "") {
			if err != nil {
				return err
			} else if !matchAny(key, glob) {
				continue
			} else if !listFlags.Long && !listFlags.JSON {
				fmt.Println(key)
				continue
			}

			rp, err := root.Open(env.Context(), s.Roots(), key)
			if err != nil {
				return err
			}
			if listFlags.JSON {
				var st index.Stats

				// For "long" JSON, include details about the index, if there is one.
				if listFlags.Long && rp.IndexKey != "" {
					idx, err := config.LoadIndex(env.Context(), s.Files(), rp.IndexKey)
					if err != nil {
						log.Printf("WARNING: loading index for %q: %v (continuing)", key, err)
					} else {
						st = idx.Stats()
					}
				}
				data, _ := json.Marshal(struct {
					S  string `json:"storageKey"`
					D  string `json:"description,omitzero"`
					F  []byte `json:"fileKey,omitzero"`
					X  []byte `json:"indexKey,omitzero"`
					IK int    `json:"numKeys,omitzero"`
					IF int    `json:"indexBits,omitzero"`
				}{key, rp.Description, []byte(rp.FileKey), []byte(rp.IndexKey), st.NumKeys, st.FilterBits})
				fmt.Println(string(data))
			} else if listFlags.Long {
				fmt.Fprint(w, key, "\t")
				if rp.IndexKey == "" {
					fmt.Fprint(w, "[-]")
				} else {
					fmt.Fprint(w, "[+]")
				}
				fmt.Fprint(w, "\t", config.PrintableKey(rp.FileKey))
				if rp.Description != "" {
					fmt.Fprint(w, "\t", rp.Description)
				}
				fmt.Fprintln(w)
			}
		}
		return nil
	})
}

var createFlags struct {
	Replace bool   `flag:"replace,Replace an existing root name"`
	Desc    string `flag:"desc,Set the human-readable description"`
	Ref     bool   `flag:"ref,Treat the target as a root/path or file/path"`
	Put     bool   `flag:"put,Treat the target as a local filesystem path to copy"`
}

func runCreate(env *command.Env, name string, rest ...string) error {
	mode := "empty"
	if len(rest) == 1 {
		if createFlags.Ref && createFlags.Put {
			return env.Usagef("the --ref and --put flags are mutually exclusive")
		} else if createFlags.Ref {
			mode = "ref"
		} else if createFlags.Put {
			mode = "put"
		} else {
			mode = "file-key"
		}
	} else if len(rest) != 0 {
		return env.Usagef("invalid arguments")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		if !createFlags.Replace {
			r, err := s.Roots().Has(env.Context(), name)
			if err != nil {
				return err
			} else if r.Has(name) {
				return fmt.Errorf("root %q already exists", name)
			}
		}

		var fk string
		var err error

		switch mode {
		case "file-key":
			fk, err = filetree.ParseKey(rest[0])
		case "ref":
			tf, terr := filetree.OpenPath(env.Context(), s, rest[0])
			if terr != nil {
				return terr
			}
			fk = tf.File.Key()
		case "put":
			tf, terr := putlib.Default.PutPath(env.Context(), s.Files(), rest[0])
			if terr != nil {
				return terr
			}
			fk, terr = tf.Flush(env.Context())
			if terr != nil {
				return terr
			}
			fmt.Printf("put: %s\n", config.FormatKey(fk))
		case "empty":
			fk, err = file.New(s.Files(), &file.NewOptions{
				Stat:        &file.Stat{Mode: os.ModeDir | 0755},
				PersistStat: true,
			}).Flush(env.Context())
		default:
			panic("unexpected mode: " + mode)
		}
		if err != nil {
			return err
		} else if _, err := file.Open(env.Context(), s.Files(), fk); err != nil {
			return err
		}

		return root.New(s.Roots(), &root.Options{
			Description: createFlags.Desc,
			FileKey:     fk,
		}).Save(env.Context(), name)
	})
}

var copyFlags struct {
	Replace bool `flag:"replace,Replace an existing target root name"`
}

func runCopy(env *command.Env, src, dst string) error {
	na, err := getNameArgs(env, env.Args)
	if err != nil {
		return err
	} else if na.Args[0] == na.Key {
		return fmt.Errorf("target %q has the same name as the source", na.Args[0])
	}
	defer na.Close()

	if !copyFlags.Replace {
		r, err := na.Store.Roots().Has(env.Context(), dst)
		if err != nil {
			return err
		} else if r.Has(dst) {
			return fmt.Errorf("root %q: already exists", dst)
		}
	}

	if err := na.Root.Save(env.Context(), na.Args[0]); err != nil {
		return err
	} else if env.Command.Name == "rename" {
		return na.Store.Roots().Delete(env.Context(), na.Key)
	}
	return nil
}

func runDelete(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing root-key arguments")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		roots := s.Roots()
		for _, key := range env.Args {
			if err := roots.Delete(env.Context(), key); err != nil {
				return fmt.Errorf("delete root %q: %w", key, err)
			}
			fmt.Println(key)
		}
		return nil
	})
}

func runEditDesc(env *command.Env, target string, rest ...string) error {
	na, err := getNameArgs(env, env.Args)
	if err != nil {
		return err
	}
	defer na.Close()
	na.Root.Description = strings.Join(na.Args, " ")
	return na.Root.Save(env.Context(), na.Key)
}

func runEditFile(env *command.Env, root, target string) error {
	na, err := getNameArgs(env, env.Args)
	if err != nil {
		return err
	}
	defer na.Close()

	var key string
	if len(na.Args) != 1 {
		return env.Usagef("incorrect arguments")
	} else {
		key, err = filetree.ParseKey(na.Args[0])
	}
	if err != nil {
		return err
	} else if _, err := file.Open(env.Context(), na.Store.Files(), key); err != nil {
		return err
	}

	if key != na.Root.FileKey {
		na.Root.IndexKey = "" // invalidate the index
	}
	na.Root.FileKey = key
	return na.Root.Save(env.Context(), na.Key)
}

type rootArgs struct {
	Key   string
	Args  []string
	Root  *root.Root
	Store filetree.Store
	Close func()
}

func getNameArgs(env *command.Env, args []string) (*rootArgs, error) {
	if len(args) < 2 {
		return nil, env.Usagef("incorrect arguments")
	}
	key := args[0]
	cfg := env.Config.(*config.Settings)
	bs, err := cfg.OpenStore(env.Context())
	if err != nil {
		return nil, err
	}
	rp, err := root.Open(env.Context(), bs.Roots(), key)
	if err != nil {
		bs.Close(env.Context())
		return nil, err
	}
	return &rootArgs{
		Key:   key,
		Args:  args[1:],
		Root:  rp,
		Store: bs,
		Close: func() { bs.Close(env.Context()) },
	}, nil
}
