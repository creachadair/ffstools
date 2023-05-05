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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"text/tabwriter"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"
)

var Command = &command.C{
	Name: "root",
	Help: "Manipulate filesystem root pointers.",

	Commands: []*command.C{
		{
			Name:  "list",
			Usage: "[name-glob]",
			Help: `List the root keys known in the store.

If a glob is provided, only names matching the glob are listed; otherwise all
known keys are listed.`,

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) { flax.MustBind(fs, &listFlags) },
			Run:      runList,
		},
		{
			Name:  "create",
			Usage: "<name>\n<name> <file-key>",
			Help: `Create a root pointer.

If only a <name> is given, a new empty root pointer is created with that name.
If a <file-key> is specified, the new root points to that file (which must exist).`,

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) { flax.MustBind(fs, &createFlags) },
			Run:      runCreate,
		},
		{
			Name:  "copy",
			Usage: "<source-name> <target-name>",
			Help:  "Duplicate a root pointer under a new name.",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) { flax.MustBind(fs, &copyFlags) },
			Run:      runCopy,
		},
		{
			Name:  "rename",
			Usage: "<source-name> <target-name>",
			Help:  "Rename a root pointer (equivalent to copy + remove).",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) { flax.MustBind(fs, &copyFlags) },
			Run:      runCopy,
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
			Usage: "<name> <file-key>",
			Help: `Edit the file key of the given root.

If a <file-key> is specified, it must already exist in the store.`,

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

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) { flax.MustBind(fs, &indexFlags) },
			Run:      runIndex,
		},
	},
}

var listFlags struct {
	Long bool `flag:"long,Print details for each root"`
	JSON bool `flag:"json,Format output as JSON"`
}

func runList(env *command.Env) error {
	if len(env.Args) > 1 {
		return env.Usagef("extra arguments after command")
	} else if len(env.Args) == 0 {
		env.Args = append(env.Args, "*")
	}
	glob := env.Args[0]

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s config.CAS) error {
		w := tabwriter.NewWriter(os.Stdout, 4, 2, 1, ' ', 0)
		defer w.Flush()

		return s.Roots().List(cfg.Context, "", func(key string) error {
			if ok, _ := path.Match(glob, key); !ok {
				return nil
			} else if !listFlags.Long && !listFlags.JSON {
				fmt.Println(key)
				return nil
			}

			rp, err := root.Open(cfg.Context, s.Roots(), key)
			if err != nil {
				return err
			}
			if listFlags.Long {
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
			} else {
				data, _ := json.Marshal(struct {
					S string `json:"storageKey"`
					D string `json:"description,omitempty"`
					F []byte `json:"fileKey,omitempty"`
					X []byte `json:"indexKey,omitempty"`
				}{key, rp.Description, []byte(rp.FileKey), []byte(rp.IndexKey)})
				fmt.Println(string(data))
			}
			return nil
		})
	})
}

var createFlags struct {
	Replace bool   `flag:"replace,Replace an existing root name"`
	Desc    string `flag:"desc,Set the human-readable description"`
}

func runCreate(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing <name> argument")
	}
	name, mode := env.Args[0], "empty"
	if len(env.Args) == 2 {
		mode = "file-key"
	} else if len(env.Args) != 1 {
		return env.Usagef("invalid arguments")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s config.CAS) error {
		var fk string
		var err error

		switch mode {
		case "file-key":
			fk, err = config.ParseKey(env.Args[1])
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

		return root.New(s.Roots(), &root.Options{
			Description: createFlags.Desc,
			FileKey:     fk,
		}).Save(cfg.Context, name, createFlags.Replace)
	})
}

var copyFlags struct {
	Replace bool `flag:"replace,Replace an existing target root name"`
}

func runCopy(env *command.Env) error {
	na, err := getNameArgs(env, env.Args)
	if err != nil {
		return err
	} else if na.Args[0] == na.Key {
		return fmt.Errorf("target %q has the same name as the source", na.Args[0])
	}
	defer na.Close()
	if err := na.Root.Save(na.Context, na.Args[0], copyFlags.Replace); err != nil {
		return err
	} else if env.Command.Name == "rename" {
		return na.Store.Roots().Delete(na.Context, na.Key)
	}
	return nil
}

func runDelete(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing root-key arguments")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s config.CAS) error {
		roots := s.Roots()
		for _, key := range env.Args {
			if err := roots.Delete(cfg.Context, key); err != nil {
				return fmt.Errorf("delete root %q: %w", key, err)
			}
			fmt.Println(key)
		}
		return nil
	})
}

func runEditDesc(env *command.Env) error {
	na, err := getNameArgs(env, env.Args)
	if err != nil {
		return err
	}
	defer na.Close()
	na.Root.Description = strings.Join(na.Args, " ")
	return na.Root.Save(na.Context, na.Key, true)
}

func runEditFile(env *command.Env) error {
	na, err := getNameArgs(env, env.Args)
	if err != nil {
		return err
	}
	defer na.Close()

	var key string
	if len(na.Args) != 1 {
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
	Store   config.CAS
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
	rp, err := root.Open(cfg.Context, bs.Roots(), key)
	if err != nil {
		bs.Close(cfg.Context)
		return nil, err
	}
	return &rootArgs{
		Context: cfg.Context,
		Key:     key,
		Args:    args[1:],
		Root:    rp,
		Store:   bs,
		Close:   func() { bs.Close(cfg.Context) },
	}, nil
}
