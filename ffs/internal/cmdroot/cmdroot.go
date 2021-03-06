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
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffstools/ffs/config"
)

var Command = &command.C{
	Name: "root",
	Help: "Manipulate filesystem root pointers",

	Commands: []*command.C{
		{
			Name:  "show",
			Usage: "<root-key>",
			Help:  "Print the representation of a filesystem root.",

			Run: runShow,
		},
		{
			Name: "list",
			Help: "List the root keys known in the store.",

			Run: runList,
		},
		{
			Name:  "create",
			Usage: "<name> <description>...",
			Help:  "Create a new (empty) root pointer.",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&createFlags.Replace, "replace", false, "Replace an existing root name")
				fs.StringVar(&createFlags.FileKey, "key", "", "Initial file key")
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
			Usage: "<name> <file-key>",
			Help:  "Edit the file key of the given root",

			Run: runEditFile,
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
			// N.B. Root keys do not need a @ prefix here, but tolerate one to
			// make it easier to go back and forth with the file command.
			clean := strings.TrimPrefix(key, "@")
			rp, err := root.Open(cfg.Context, config.Roots(s), clean)
			if err != nil {
				fmt.Fprintf(env, "Error: %v\n", err)
				lastErr = err
				continue
			}
			msg := root.Encode(rp).Value.(*wiretype.Object_Root).Root
			fmt.Println(config.ToJSON(map[string]interface{}{
				"storageKey": config.PrintableKey(clean),
				"root":       msg,
			}))
		}
		return lastErr
	})
}

func runList(env *command.Env, args []string) error {
	if len(args) != 0 {
		return env.Usagef("extra arguments after command")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		return config.Roots(s).List(cfg.Context, "", func(key string) error {
			fmt.Println(key)
			return nil
		})
	})
}

var createFlags struct {
	Replace bool
	FileKey string
}

func runCreate(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("usage is: <name> <description>...")
	}
	key := args[0]
	desc := strings.Join(args[1:], " ")

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		var fk string
		var err error

		if createFlags.FileKey == "" {
			fk, err = file.New(s, &file.NewOptions{
				Stat: &file.Stat{Mode: os.ModeDir | 0755},
			}).Flush(cfg.Context)
		} else {
			fk, err = config.ParseKey(createFlags.FileKey)
		}
		if err != nil {
			return err
		}
		return root.New(config.Roots(s), &root.Options{
			Description: desc,
			FileKey:     fk,
		}).Save(cfg.Context, key, createFlags.Replace)
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

	key, err := config.ParseKey(na.Args[0])
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
