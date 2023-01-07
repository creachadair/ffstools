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
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
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
				fs.BoolVar(&listFlags.Key, "key", false, "Include storage keys")
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
If the origin is from a root, the root is updated with the changes.
`,

			Run: runRemove,
		},
		{
			Name: "set-stat",
			Usage: `<root-key>/<path> <stat-spec>
@<origin-key>/<path> <stat-spec>`,
			Help: `Modify the stat message of the specified path beneath the origin

The stat spec is a list of fields to update, one or more of:

 mode <perms>   -- set file permissions (e.g., 0755)
 mtime <time>   -- update file timestamp ("now", @<seconds>, or RFC3339)
 uid <id>       -- set the owner UID
 gid <id>       -- set the group GID
 owner <name>   -- set the owner name ("" to clear)
 group <name>   -- set the group name ("" to clear)
 persist <ok>   -- set or unset stat persistence

If the origin is from a root, the root is updated with the changes.`,

			Run: runSetStat,
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
	Key     bool
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
	skey, xtag, xattrs := "", " ", ""
	hasXAttr := f.XAttr().Len() != 0
	if hasXAttr {
		xtag = "@"
		if listFlags.XAttr {
			xattrs = "\f"
			xa := f.XAttr()
			for _, key := range xa.Names() {
				xattrs += fmt.Sprintf("\t%s\t%d\n", key, len(xa.Get(key)))
			}
			xattrs = strings.TrimRight(xattrs, "\n")
		}
	}
	if listFlags.Key {
		skey = base64.StdEncoding.EncodeToString([]byte(f.Key())) + "\t"
	}

	return fmt.Sprintf("%s%s%s\t%3d\t%-8s\t%-8s\v%8d\t%s\t%s%s\f",
		skey, s.Mode, xtag, 1+f.Child().Len(),
		nameOrID(s.OwnerName, s.OwnerID), nameOrID(s.GroupName, s.GroupID),
		f.Data().Size(), date, name, xattrs)
}

func jsonFormat(f *file.File, name, target string) string {
	s := f.Stat()
	tag := strings.ToLower(s.Mode.Type().String()[:1])
	var xattr map[string][]byte
	if listFlags.XAttr {
		xattr = make(map[string][]byte)
		xa := f.XAttr()
		for _, key := range xa.Names() {
			xattr[key] = []byte(xa.Get(key))
		}
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
		Key    []byte            `json:"storageKey,omitempty"`
		XAttr  map[string][]byte `json:"xattr,omitempty"`
	}{
		Name: name,
		Type: tag, Mode: int64(s.Mode.Perm()), NLinks: 1 + f.Child().Len(),
		Owner: nameOrID(s.OwnerName, s.OwnerID), Group: nameOrID(s.GroupName, s.GroupID),
		Size: f.Data().Size(), MTime: s.ModTime.UTC(),
		Target: target, Key: []byte(f.Key()), XAttr: xattr,
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

func runSetStat(env *command.Env, args []string) error {
	if len(args) < 3 {
		return env.Usagef("missing origin and stat spec")
	}
	path := args[0]
	mod, err := parseStatMod(args[1:])
	if err != nil {
		return fmt.Errorf("invalid mod spec: %w", err)
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		tf, err := config.OpenPath(cfg.Context, s, path)
		if err != nil {
			return err
		}
		stat := tf.File.Stat()
		if mod.perms != nil {
			stat.Mode = (stat.Mode &^ fs.ModePerm) | fs.FileMode(*mod.perms)
		}
		if mod.modTime != nil {
			stat.ModTime = *mod.modTime
		}
		if mod.uid != nil {
			stat.OwnerID = *mod.uid
		}
		if mod.gid != nil {
			stat.GroupID = *mod.gid
		}
		if mod.owner != nil {
			stat.OwnerName = *mod.owner
		}
		if mod.group != nil {
			stat.GroupName = *mod.group
		}
		if mod.persist != nil {
			stat.Persist(*mod.persist)
		}
		stat.Update()
		key, err := tf.Flush(cfg.Context)
		if err != nil {
			return err
		}
		fmt.Printf("%x\n", key)
		return nil
	})
}

type statMod struct {
	perms        *int64
	modTime      *time.Time
	uid, gid     *int
	owner, group *string
	persist      *bool
}

func parseStatMod(args []string) (*statMod, error) {
	var mod statMod

	i := 0
	for i+1 < len(args) {
		switch args[i] {
		case "mode":
			v, err := strconv.ParseInt(args[i+1], 0, 32)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.perms = &v

		case "mtime":
			var t time.Time
			if args[i+1] == "now" {
				t = time.Now()

			} else if strings.HasPrefix(args[i+1], "@") {
				v, err := strconv.ParseFloat(args[i+1][1:], 64)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", args[i], err)
				}
				sec, rem := math.Modf(v)
				nano := float64(time.Second) * rem
				t = time.Unix(int64(sec), int64(nano))

			} else if v, err := time.Parse(time.RFC3339Nano, args[i+1]); err == nil {
				t = v

			} else {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.modTime = &t

		case "uid", "gid":
			v, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			} else if args[i] == "uid" {
				mod.uid = &v
			} else {
				mod.gid = &v
			}

		case "owner":
			mod.owner = &args[i+1]

		case "group":
			mod.group = &args[i+1]

		case "persist":
			v, err := strconv.ParseBool(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.persist = &v

		default:
			return nil, fmt.Errorf("unknown stat field %q", args[i])
		}

		i += 2
	}
	if i < len(args) {
		return nil, errors.New("odd-length argument list")
	}
	return &mod, nil
}
