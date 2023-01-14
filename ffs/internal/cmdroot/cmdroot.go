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

package cmdroot

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"text/tabwriter"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/putlib"
)

var Command = &command.C{
	Name: "root",
	Help: "Manipulate filesystem root pointers.",

	Commands: []*command.C{
		{
			Name:  "show",
			Usage: "<root-key>",
			Help:  "Print the representation of a filesystem root.",

			Run: runShow,
		},
		{
			Name:  "list",
			Usage: "[name-glob]",
			Help: `List the root keys known in the store.
If a glob is provided, only names matching the glob are listed; otherwise all
known keys are listed.`,

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&listFlags.Long, "long", false, "Print details for each root")
			},
			Run: runList,
		},
		{
			Name:  "create",
			Usage: "<name>\n<name> <file-key>\n<name> put <path>",
			Help: `Create a root pointer.

If only a <name> is given, a new empty root pointer is created with that name.
If a <file-key> is specified, the new root points to that file (which must exist).
The "put <path>" form puts the specified path into the store, and uses the resulting
storage key (see the "put" subcommmand).`,

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&createFlags.Replace, "replace", false, "Replace an existing root name")
				fs.StringVar(&createFlags.Desc, "desc", "", "Set the human-readable description")
			},
			Run: runCreate,
		},
		{
			Name:  "copy",
			Usage: "<source-name> <target-name>",
			Help:  "Duplicate a root pointer under a new name.",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&copyFlags.Replace, "replace", false, "Replace an existing target root name")
			},
			Run: runCopy,
		},
		{
			Name:  "rename",
			Usage: "<source-name> <target-name>",
			Help:  "Rename a root pointer (equivalent to copy + remove).",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&copyFlags.Replace, "replace", false, "Replace an existing target root name")
			},
			Run: runCopy,
		},
		{
			Name:  "delete",
			Usage: "<root-key> ...",
			Help:  "Delete the specified root pointers.",

			Run: runDelete,
		},
		{
			Name:  "set-description",
			Usage: "<name> <description>...",
			Help:  "Edit the description of the given root pointer.",

			Run: runEditDesc,
		},
		{
			Name:  "set-file",
			Usage: "<name> <file-key>\n<name> put <path>",
			Help: `Edit the file key of the given root.

If a <file-key> is specified, it must already exist in the store.

The "put <path>" form stores the specified path into the store, and uses the
resulting key (see the "put" subcommand).`,

			Run: runEditFile,
		},
		{
			Name:  "index",
			Usage: "<root-key> ...",
			Help: `
Update each of the specified roots to include a blob index.

An index is a Bloom filter of the keys reachable from the root.  If a root
already has an index, it is not changed; use -f to force a new index to be
computed anyway.`,

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&indexFlags.Force, "f", false, "Force reindexing")
			},

			Run: runIndex,
		},
	},
}

func runShow(env *command.Env, keys []string) error {
	if len(keys) == 0 {
		return env.Usagef("missing required <root-key>")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		var lastErr error
		for _, key := range keys {
			rp, err := root.Open(cfg.Context, config.Roots(s), key)
			if err != nil {
				fmt.Fprintf(env, "Error: %v\n", err)
				lastErr = err
				continue
			}
			msg := root.Encode(rp).Value.(*wiretype.Object_Root).Root
			fmt.Println(config.ToJSON(map[string]interface{}{
				"storageKey": config.PrintableKey(key),
				"root":       msg,
			}))
		}
		return lastErr
	})
}

var listFlags struct {
	Long bool
}

func runList(env *command.Env, args []string) error {
	if len(args) > 1 {
		return env.Usagef("extra arguments after command")
	} else if len(args) == 0 {
		args = append(args, "*")
	}
	glob := args[0]

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		w := tabwriter.NewWriter(os.Stdout, 4, 2, 1, ' ', 0)
		defer w.Flush()

		return config.Roots(s).List(cfg.Context, "", func(key string) error {
			if ok, _ := path.Match(glob, key); !ok {
				return nil
			} else if !listFlags.Long {
				fmt.Println(key)
				return nil
			}
			rp, err := root.Open(cfg.Context, config.Roots(s), key)
			if err != nil {
				return err
			}
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
			return nil
		})
	})
}

var createFlags struct {
	Replace bool
	Desc    string
}

func runCreate(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing <name> argument")
	}
	name, mode := args[0], "empty"
	if len(args) == 2 {
		mode = "file-key"
	} else if len(args) == 3 && args[1] == "put" {
		mode = "put-path"
	} else if len(args) != 1 {
		return env.Usagef("invalid arguments")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		var fk string
		var err error

		switch mode {
		case "file-key":
			fk, err = config.ParseKey(args[1])
		case "put-path":
			f, perr := putlib.Default.PutPath(cfg.Context, s, args[2])
			if perr != nil {
				return perr
			}
			fk, err = f.Flush(cfg.Context)
			if err == nil {
				fmt.Printf("put: %x\n", fk)
			}
		case "empty":
			fk, err = file.New(s, &file.NewOptions{
				Stat: &file.Stat{Mode: os.ModeDir | 0755},
			}).Flush(cfg.Context)
		default:
			panic("unexpected mode: " + mode)
		}
		if err != nil {
			return err
		} else if _, err := file.Open(cfg.Context, s, fk); err != nil {
			return err
		}

		return root.New(config.Roots(s), &root.Options{
			Description: createFlags.Desc,
			FileKey:     fk,
		}).Save(cfg.Context, name, createFlags.Replace)
	})
}

var copyFlags struct {
	Replace bool
}

func runCopy(env *command.Env, args []string) error {
	na, err := getNameArgs(env, args)
	if err != nil {
		return err
	} else if na.Args[0] == na.Key {
		return fmt.Errorf("target %q has the same name as the source", na.Args[0])
	}
	defer na.Close()
	if err := na.Root.Save(na.Context, na.Args[0], copyFlags.Replace); err != nil {
		return err
	} else if env.Command.Name == "rename" {
		return config.Roots(na.Store).Delete(na.Context, na.Key)
	}
	return nil
}

func runDelete(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing root-key arguments")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		roots := config.Roots(s)
		for _, key := range args {
			if err := roots.Delete(cfg.Context, key); err != nil {
				return fmt.Errorf("delete root %q: %w", key, err)
			}
			fmt.Println(key)
		}
		return nil
	})
}

func runEditDesc(env *command.Env, args []string) error {
	na, err := getNameArgs(env, args)
	if err != nil {
		return err
	}
	defer na.Close()
	na.Root.Description = strings.Join(na.Args, " ")
	return na.Root.Save(na.Context, na.Key, true)
}

func runEditFile(env *command.Env, args []string) error {
	na, err := getNameArgs(env, args)
	if err != nil {
		return err
	}
	defer na.Close()

	var key string
	if len(na.Args) == 2 && na.Args[0] == "put" {
		f, err := putlib.Default.PutPath(na.Context, na.Store, na.Args[1])
		if err != nil {
			return err
		}
		key, err = f.Flush(na.Context)
		if err != nil {
			return err
		}
		fmt.Printf("put: %x\n", key)
	} else if len(na.Args) != 1 {
		return env.Usagef("incorrect arguments")
	} else {
		key, err = config.ParseKey(na.Args[0])
	}
	if err != nil {
		return err
	} else if _, err := file.Open(na.Context, na.Store, key); err != nil {
		return err
	}

	if key != na.Root.FileKey {
		na.Root.IndexKey = "" // invalidate the index
	}
	na.Root.FileKey = key
	return na.Root.Save(na.Context, na.Key, true)
}

type rootArgs struct {
	Context context.Context
	Key     string
	Args    []string
	Root    *root.Root
	Store   blob.CAS
	Close   func()
}

func getNameArgs(env *command.Env, args []string) (*rootArgs, error) {
	if len(args) < 2 {
		return nil, env.Usagef("incorrect arguments")
	}
	key := args[0]
	cfg := env.Config.(*config.Settings)
	bs, err := cfg.OpenStore()
	if err != nil {
		return nil, err
	}
	rp, err := root.Open(cfg.Context, config.Roots(bs), key)
	if err != nil {
		blob.CloseStore(cfg.Context, bs)
		return nil, err
	}
	return &rootArgs{
		Context: cfg.Context,
		Key:     key,
		Args:    args[1:],
		Root:    rp,
		Store:   bs,
		Close:   func() { blob.CloseStore(cfg.Context, bs) },
	}, nil
}
