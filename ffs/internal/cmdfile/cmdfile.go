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

package cmdfile

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/putlib"
)

const fileCmdUsage = `<root-key>[/path] ...
@<file-key>[/path] ...`

var Command = &command.C{
	Name: "file",
	Help: `Manipulate file and directory objects.

File objects are addressed by storage keys. The storage key for
a file may be specified in the following formats:

  <root-name>                   : the file key from a root pointer
  @74686973206973206d79206b6579 : hexadecimal encoded
  @dGhpcyBpcyBteSBrZXk=         : base64 encoded
`,

	Commands: []*command.C{
		{
			Name:  "show",
			Usage: fileCmdUsage,
			Help:  "Print the representation of a file object",

			Run: runShow,
		},
		{
			Name:  "ls",
			Usage: fileCmdUsage,
			Help:  "List file attributes in a style similar to the ls command",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&listFlags.DirOnly, "d", false, "List directories as plain files")
				fs.BoolVar(&listFlags.All, "a", false, "Include entries whose names begin with dot (.)")
				fs.BoolVar(&listFlags.XAttr, "xattr", false, "Include extended attributes")
				fs.BoolVar(&listFlags.JSON, "json", false, "Emit output in JSON format")
			},
			Run: runList,
		},
		{
			Name:  "read",
			Usage: fileCmdUsage,
			Help:  "Read the binary contents of a file object",

			Run: runRead,
		},
		{
			Name: "set",
			Usage: `<root-key>/<path> (<target> | put <path>)
@<origin-key>/<path> (<target> | put <path>)`,
			Help: `Set the specified path beneath the origin to the given target

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.

The <target> may be a root-key/path or a @file-key/path. In both cases the path
component is optional; if a root-key is given alone its root file is used as
the target.

If the target is "put <path>", the specified path is put into the store, and
the resulting storage key is used (see the "put" subcommand).`,

			Run: runSet,
		},
		{
			Name: "remove",
			Usage: `<root-key>/<path> ...
@<origin-key>/<path> ...`,
			Help: `Remove the specified path from beneath the origin

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.
`,

			Run: runRemove,
		},
	},
}

func runShow(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		for _, arg := range args {
			if arg == "" {
				return env.Usagef("origin may not be empty")
			}
			of, err := config.OpenPath(cfg.Context, s, arg)
			if err != nil {
				return err
			}

			msg := file.Encode(of.File).Value.(*wiretype.Object_Node).Node
			fmt.Println(config.ToJSON(map[string]interface{}{
				"storageKey": []byte(of.FileKey),
				"node":       msg,
			}))
		}
		return nil
	})
}

var listFlags struct {
	DirOnly bool
	All     bool
	XAttr   bool
	JSON    bool
}

func runList(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		w := tabwriter.NewWriter(os.Stdout, 2, 2, 1, ' ', 0)
		defer w.Flush()

		for _, arg := range args {
			if arg == "" {
				return env.Usagef("origin may not be empty")
			}
			pi, err := config.OpenPath(cfg.Context, s, arg)
			if err != nil {
				return err
			}
			of := pi.File
			name := path.Base(pi.Path)

			// List an individual file or directory name.
			printOne := func(of *file.File, name string) error {
				// Skip dot files unless -a is set.
				if strings.HasPrefix(name, ".") && !listFlags.All {
					return nil
				}

				target, err := linkTarget(cfg.Context, of)
				if err != nil {
					return err
				}
				if listFlags.JSON {
					fmt.Println(jsonFormat(of, name, target))
				} else {
					fmt.Fprint(w, listFormat(of, name, target))
				}
				return nil
			}

			// List contents of directories unless -d is set.
			if of.Stat().Mode.IsDir() && !listFlags.DirOnly {
				for _, kid := range of.Child().Names() {
					if strings.HasPrefix(kid, ".") && !listFlags.All {
						continue
					}

					cf, err := of.Open(cfg.Context, kid)
					if err != nil {
						return fmt.Errorf("open %q: %w", kid, err)
					} else if err := printOne(cf, kid); err != nil {
						return err
					}
				}
				continue
			}
			if err := printOne(pi.File, name); err != nil {
				return err
			}
		}
		return nil
	})
}

func listFormat(f *file.File, name, target string) string {
	s := f.Stat()
	var date string
	if now := time.Now(); now.Year() != s.ModTime.Year() {
		date = s.ModTime.Format("Jan _2  2006")
	} else {
		date = s.ModTime.Format("Jan _2 15:04")
	}
	if target != "" {
		name += " -> " + target
	}
	xtag, xattrs := " ", ""
	hasXAttr := f.XAttr().Len() != 0
	if listFlags.XAttr && hasXAttr {
		xattrs = "\f"
		f.XAttr().List(func(key, value string) {
			xattrs += fmt.Sprintf("\t%s\t%d\n", key, len(value))
		})
		xattrs = strings.TrimRight(xattrs, "\n")
	}
	if hasXAttr {
		xtag = "@"
	}
	return fmt.Sprintf("%s%s\t%2d\t%-8s\t%-8s\v%8d\t%s\t%s%s\f",
		s.Mode, xtag, 1+f.Child().Len(),
		nameOrID(s.OwnerName, s.OwnerID), nameOrID(s.GroupName, s.GroupID),
		f.Size(), date, name, xattrs)
}

func jsonFormat(f *file.File, name, target string) string {
	s := f.Stat()
	tag := strings.ToLower(s.Mode.Type().String()[:1])
	var xattr map[string][]byte
	if listFlags.XAttr {
		xattr = make(map[string][]byte)
		f.XAttr().List(func(key, value string) { xattr[key] = []byte(value) })
	}
	data, err := json.Marshal(struct {
		Name   string            `json:"name"`
		Type   string            `json:"type"`
		Mode   int64             `json:"mode"`
		NLinks int               `json:"nLinks"`
		Owner  string            `json:"owner"`
		Group  string            `json:"group"`
		Size   int64             `json:"size"`
		MTime  time.Time         `json:"modTime"`
		Target string            `json:"linkTarget,omitempty"`
		XAttr  map[string][]byte `json:"xattr,omitempty"`
	}{
		Name: name,
		Type: tag, Mode: int64(s.Mode.Perm()), NLinks: 1 + f.Child().Len(),
		Owner: nameOrID(s.OwnerName, s.OwnerID), Group: nameOrID(s.GroupName, s.GroupID),
		Size: f.Size(), MTime: s.ModTime.UTC(), Target: target, XAttr: xattr,
	})
	if err != nil {
		return "null"
	}
	return string(data)
}

func linkTarget(ctx context.Context, f *file.File) (string, error) {
	if f.Stat().Mode.Type()&fs.ModeSymlink != 0 {
		target, err := io.ReadAll(f.Cursor(ctx))
		if err != nil {
			return "", fmt.Errorf("reading symlink: %w", err)
		}
		return string(target), nil
	}
	return "", nil
}

func nameOrID(name string, id int) string {
	if name != "" {
		return name
	}
	idstr := strconv.Itoa(id)
	if u, err := user.LookupId(idstr); err == nil {
		return u.Username
	} else if g, err := user.LookupGroupId(idstr); err == nil {
		return g.Name
	}
	return idstr
}

func runRead(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		of, err := config.OpenPath(cfg.Context, s, args[0])
		if err != nil {
			return err
		}
		r := bufio.NewReaderSize(of.File.Cursor(cfg.Context), 1<<20)
		_, err = io.Copy(os.Stdout, r)
		return err
	})
}

func runSet(env *command.Env, args []string) error {
	if len(args) == 3 {
		if args[1] != "put" {
			return env.Usagef("invalid three-argument usage")
		}
	} else if len(args) != 2 {
		return env.Usagef("got %d arguments, wanted origin/path, target", len(args))
	}

	obase, orest := config.SplitPath(args[0])
	if orest == "" {
		return env.Usagef("path must not be empty")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		of, err := config.OpenPath(cfg.Context, s, obase) // N.B. No path; see below
		if err != nil {
			return err
		}

		var tf *config.PathInfo
		if len(args) == 2 {
			// Standard form: file-key/path or root-key/path
			tf, err = config.OpenPath(cfg.Context, s, args[1])
		} else {
			// Put form: put <path>
			f, perr := putlib.Default.PutPath(cfg.Context, s, args[2])
			if perr != nil {
				return perr
			}
			fk, perr := f.Flush(cfg.Context)
			if perr != nil {
				return perr
			}
			fmt.Printf("put: %x\n", fk)
			tf = &config.PathInfo{
				Base:    f,
				File:    f,
				FileKey: fk,
			}
		}
		if err != nil {
			return err
		}

		if _, err := fpath.Set(cfg.Context, of.Base, orest, &fpath.SetOptions{
			Create: true,
			SetStat: func(st *file.Stat) {
				if st.Mode == 0 {
					st.Mode = fs.ModeDir | 0755
				}
			},
			File: tf.File,
		}); err != nil {
			return err
		}
		key, err := of.Flush(cfg.Context)
		if err != nil {
			return err
		}
		fmt.Printf("%x\n", key)
		return nil
	})
}

func runRemove(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing origin/path")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		for _, arg := range args {
			base, rest := config.SplitPath(arg)
			if rest == "" {
				return fmt.Errorf("missing path %q", arg)
			}
			of, err := config.OpenPath(cfg.Context, s, base) // N.B. No path; see below
			if err != nil {
				return err
			}

			if err := fpath.Remove(cfg.Context, of.Base, rest); err != nil {
				return err
			}
			key, err := of.Flush(cfg.Context)
			if err != nil {
				return err
			}
			fmt.Printf("%x\n", key)
		}
		return nil
	})
}
