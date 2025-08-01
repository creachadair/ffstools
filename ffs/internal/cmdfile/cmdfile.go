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

// Package cmdfile implements the "ffs file" subcommand.
package cmdfile

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/putlib"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/mapset"
	"github.com/creachadair/mds/value"
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
			Help:  "Print the representation of a file object.",

			SetFlags: command.Flags(flax.MustBind, &showFlags),
			Run:      runShow,
		},
		{
			Name:  "list",
			Usage: fileCmdUsage,
			Help:  "List file attributes in a style similar to the ls command.",

			SetFlags: command.Flags(flax.MustBind, &listFlags),
			Run:      runList,
		},
		{
			Name:  "read",
			Usage: fileCmdUsage,
			Help:  "Read the binary contents of a file object",

			Run: command.Adapt(runRead),
		},
		{
			Name: "set",
			Usage: `<root-key>/<path> <target>
@<origin-key>/<path> <target>`,
			Help: `Set the specified path beneath the origin to the given target.

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.

The <target> may be a root-key/path or a @file-key/path. In both cases the path
component is optional; if a root-key is given alone its root file is used as
the target.`,

			Run: command.Adapt(runSet),
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
			Help: `Modify the stat message of the specified path beneath the origin.

The stat spec is a list of fields to update, one or more of:

 mode <perms>   -- set file permissions (e.g., 0755)
 type <type>    -- set file type (see below)
 mtime <time>   -- update file timestamp ("now", @<seconds>, or RFC3339)
 uid <id>       -- set the owner UID
 gid <id>       -- set the group GID
 owner <name>   -- set the owner name ("" to clear)
 group <name>   -- set the group name ("" to clear)
 persist <ok>   -- set or unset stat persistence
 clear          -- clear all current stat values to zero (applies first)

Allowed types include:

 f, file:          regular file
 d, dir:           directory
 l, link, symlink: symbolic link
 p, pipe, fifo:    named pipe (FIFO)
 s, socket:        socket
 b, block, bdev:   block device
 c, char, cdev:    character device

If the origin is from a root, the root is updated with the changes.`,

			Run: command.Adapt(runSetStat),
		},
		{
			Name: "xattr",
			Usage: `<root-key>/<path> <xattr-spec>
@<origin-key>/<path> <xattr-spec>`,
			Help: `Edit extended attributes of the specified path beneath the origin.

The xattr spec is one of the following:

  list               -- list the extended attribute names
  clear              -- remove all extended attributes
  get <name>         -- get the value of the xattr
  delete <name>      -- delete the named xattr
  set <name> <value> -- set the value of the xattr

If the origin is from a root, the root is updated with the changes.`,

			Run: command.Adapt(runXAttr),
		},
		{
			Name:  "resolve",
			Usage: fileCmdUsage,
			Help:  "Show the storage key targeted by the specified path.",

			SetFlags: command.Flags(flax.MustBind, &resolveFlags),
			Run:      command.Adapt(runResolve),
		},
		{
			Name: "find-keys",
			Usage: `<root-key>/<path> <key> ...
@<origin-key>/<path> <key> ...`,
			Help: "Find where the specified keys are used.",

			SetFlags: command.Flags(flax.MustBind, &findFlags),
			Run:      command.Adapt(runFindKeys),
		},
		{
			Name:  "fsck",
			Usage: fileCmdUsage,
			Help:  "Check file tree integrity.",
			Run:   command.Adapt(runFileCheck),
		},
	},
}

var showFlags struct {
	Raw bool `flag:"raw,Write the file record in binary format"`
}

func runShow(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, arg := range env.Args {
			if arg == "" {
				return env.Usagef("origin may not be empty")
			}
			of, err := filetree.OpenPath(env.Context(), s, arg)
			if err != nil {
				return err
			}

			msg := file.Encode(of.File)
			if showFlags.Raw {
				bits, _ := wiretype.ToBinary(msg)
				os.Stdout.Write(bits)
			} else {
				fmt.Println(config.ToJSON(map[string]any{
					"storageKey": []byte(of.FileKey),
					"node":       msg.Value.(*wiretype.Object_Node).Node,
				}))
			}
		}
		return nil
	})
}

var listFlags struct {
	DirOnly bool `flag:"d,List directories as plain files"`
	Long    bool `flag:"long,Print detail for each file entry"`
	XAttr   bool `flag:"xattr,Include extended attributes"`
	Key     bool `flag:"key,Include storage keys"`
	JSON    bool `flag:"json,Emit output in JSON format"`
}

func runList(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		w := tabwriter.NewWriter(os.Stdout, 2, 2, 1, ' ', 0)
		defer w.Flush()

		for _, arg := range env.Args {
			if arg == "" {
				return env.Usagef("origin may not be empty")
			}
			pi, err := filetree.OpenPath(env.Context(), s, arg)
			if err != nil {
				return err
			}
			of := pi.File
			name := path.Base(pi.Path)

			// List contents of directories unless -d is set.
			if of.Stat().Mode.IsDir() && !listFlags.DirOnly {
				for _, kid := range of.Child().Names() {
					cf, err := of.Open(env.Context(), kid)
					if err != nil {
						return fmt.Errorf("open %q: %w", kid, err)
					} else if err := printOne(env.Context(), w, cf, kid); err != nil {
						return err
					}
				}
				continue
			}
			if err := printOne(env.Context(), w, pi.File, name); err != nil {
				return err
			}
		}
		return nil
	})
}

// List an individual file or directory name.
func printOne(ctx context.Context, tw io.Writer, of *file.File, name string) error {
	if !listFlags.Long && !listFlags.JSON {
		if listFlags.Key {
			fmt.Print(base64.StdEncoding.EncodeToString([]byte(of.Key())) + "\t")
		}
		fmt.Println(name)
		return nil
	}
	target, err := linkTarget(ctx, of)
	if err != nil {
		return err
	}
	if listFlags.JSON {
		fmt.Println(jsonFormat(of, name, target))
	} else {
		fmt.Fprint(tw, listFormat(of, name, target))
	}
	return nil
}

func listFormat(f *file.File, name, target string) string {
	s := f.Stat()
	size := f.Data().Size()
	if s.Mode.IsDir() {
		size = 0
		for _, kid := range f.Child().Names() {
			size += int64(32 + len(kid))
			// +32 for the storage key. This is just an estimate; the point here
			// is to have some stable number that approximates how much storage
			// the directory occupies.
		}
	}
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

	return fmt.Sprintf("%s%s%s\t%3d\t%s\t%s\v%9d\t%s\t%s%s\f",
		skey, s.Mode, xtag, 1+f.Child().Len(),
		nameOrID(s.OwnerName, s.OwnerID), nameOrID(s.GroupName, s.GroupID),
		size, date, name, xattrs,
	)
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
		Blocks int               `json:"blocks,omitempty"`
		MTime  time.Time         `json:"modTime"`
		Target string            `json:"linkTarget,omitempty"`
		Key    []byte            `json:"storageKey,omitempty"`
		XAttr  map[string][]byte `json:"xattr,omitempty"`
	}{
		Name: name,
		Type: tag, Mode: int64(s.Mode.Perm()), NLinks: 1 + f.Child().Len(),
		Owner: nameOrID(s.OwnerName, s.OwnerID), Group: nameOrID(s.GroupName, s.GroupID),
		Size: f.Data().Size(), Blocks: f.Data().Len(), MTime: s.ModTime.UTC(),
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

func runRead(env *command.Env, originPath string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := filetree.OpenPath(env.Context(), s, originPath)
		if err != nil {
			return err
		}
		r := bufio.NewReaderSize(of.File.Cursor(env.Context()), 1<<20)
		_, err = io.Copy(os.Stdout, r)
		return err
	})
}

func runSet(env *command.Env, originPath, target string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		tf, err := filetree.OpenPath(env.Context(), s, target)
		if err != nil {
			return err
		}
		key, err := putlib.SetPath(env.Context(), s, originPath, tf.File)
		if err != nil {
			return err
		}
		fmt.Printf("set: %s\n", config.FormatKey(key))
		return nil
	})
}

func runRemove(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing origin/path")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, arg := range env.Args {
			base, rest := filetree.SplitPath(arg)
			if rest == "" || rest == "." {
				return fmt.Errorf("missing path %q", arg)
			}
			of, err := filetree.OpenPath(env.Context(), s, base) // N.B. No path; see below
			if err != nil {
				return err
			}

			if err := fpath.Remove(env.Context(), of.Base, rest); err != nil {
				return err
			}
			key, err := of.Flush(env.Context())
			if err != nil {
				return err
			}
			fmt.Printf("remove: %s\n", config.FormatKey(key))
		}
		return nil
	})
}

func runSetStat(env *command.Env, path string, mods []string) error {
	if len(mods) == 0 {
		return env.Usagef("missing stat spec")
	}
	mod, err := parseStatMod(mods)
	if err != nil {
		return fmt.Errorf("invalid mod spec: %w", err)
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		tf, err := filetree.OpenPath(env.Context(), s, path)
		if err != nil {
			return err
		}
		stat := tf.File.Stat()
		if mod.clear {
			stat.Clear()
			stat = tf.File.Stat()
		}
		if mod.perms != nil {
			stat.Mode = (stat.Mode &^ fs.ModePerm) | fs.FileMode(*mod.perms)
		}
		if mod.ftype != nil {
			stat.Mode = (stat.Mode &^ fs.ModeType) | *mod.ftype
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
		key, err := tf.Flush(env.Context())
		if err != nil {
			return err
		}
		fmt.Printf("set-stat: %s\n", config.FormatKey(key))
		return nil
	})
}

func runXAttr(env *command.Env, fileSpec, op string, args ...string) error {
	if err := checkXAttrSpec(op, args); err != nil {
		return err
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := filetree.OpenPath(env.Context(), s, fileSpec)
		if err != nil {
			return err
		}
		oldKey := of.BaseKey
		xv := of.File.XAttr()

		switch op {
		case "list":
			if xv.Len() != 0 {
				fmt.Println(strings.Join(xv.Names(), "\n"))
			}
		case "clear":
			if n := xv.Len(); n != 0 {
				fmt.Fprintf(env, "removed %d xattr\n", n)
			}
			xv.Clear()
		case "get":
			if xv.Has(args[0]) {
				fmt.Println(xv.Get(args[0]))
			} else {
				fmt.Fprintf(env, "xattr %q not found\n", args[0])
			}
		case "delete":
			xv.Remove(args[0])
		case "set":
			xv.Set(args[0], args[1])
		default:
			panic("unknown xattr spec: " + op) // unreachable
		}

		key, err := of.Flush(env.Context())
		if err != nil {
			return err
		}
		if key != oldKey {
			fmt.Printf("xattr: %s\n", config.FormatKey(key))
		}
		return nil
	})
}

func checkXAttrSpec(op string, args []string) error {
	switch op {
	case "list", "clear":
		if len(args) != 0 {
			return fmt.Errorf("extra args after %q", op)
		}
	case "get", "delete":
		if len(args) != 1 {
			return fmt.Errorf("wrong number of args for %q (got %d, want 1)", op, len(args))
		}
	case "set":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of args for %q (got %d, want 2)", op, len(args))
		}
	default:
		return fmt.Errorf("unknown xattr operation %q", op)
	}
	return nil
}

var resolveFlags struct {
	Path bool `flag:"path,Show each key traversed by the path"`
}

func runResolve(env *command.Env, originPath string) error {
	cfg := env.Config.(*config.Settings)
	if !resolveFlags.Path {
		return cfg.WithStore(env.Context(), func(s filetree.Store) error {
			rf, err := filetree.OpenPath(env.Context(), s, env.Args[0])
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", config.FormatKey(rf.File.Key()))
			return nil
		})
	}
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		base, rest := filetree.SplitPath(originPath)
		rf, err := filetree.OpenPath(env.Context(), s, base) // N.B. No path; see below
		if err != nil {
			return err
		}
		if rf.RootKey != "" {
			fmt.Printf("%s %s\n", config.FormatKey(rf.Base.Key()), rf.RootKey)
		} else {
			fmt.Printf("%s\n", config.FormatKey(rf.Base.Key()))
		}
		parts := strings.Split(rest, "/")
		pf, err := fpath.OpenPath(env.Context(), rf.Base, rest)
		for i, f := range pf {
			fmt.Printf("%s %s\n", config.FormatKey(f.Key()), parts[i])
		}
		return err
	})
}

var findFlags struct {
	All bool `flag:"all,Find all occurrences"`
}

var errFindFound = errors.New("found")

func runFindKeys(env *command.Env, origin string, keys ...string) error {
	cfg := env.Config.(*config.Settings)
	var parsed []string
	for i, key := range keys {
		p, err := filetree.ParseKey(key)
		if err != nil {
			return fmt.Errorf("key %d: %w", i+1, err)
		}
		parsed = append(parsed, p)
	}

	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := filetree.OpenPath(env.Context(), s, origin)
		if err != nil {
			return err
		}
		want := mapset.New(parsed...)
		werr := fpath.Walk(env.Context(), of.File, func(e fpath.Entry) error {
			if e.Err != nil {
				return err
			}
			if want.Has(e.File.Key()) {
				fmt.Printf("file %q %s\n", e.Path, config.FormatKey(e.File.Key()))
				if !findFlags.All {
					return errFindFound
				}
			}
			for i, dkey := range e.File.Data().Keys() {
				if want.Has(dkey) {
					fmt.Printf("data %q [%d] %s\n", e.Path, i, config.FormatKey(dkey))
					if !findFlags.All {
						return errFindFound
					}
				}
			}
			return nil
		})
		if errors.Is(werr, errFindFound) {
			return nil
		}
		return werr
	})
}

func runFileCheck(env *command.Env, origins ...string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, org := range origins {
			of, err := filetree.OpenPath(env.Context(), s, org)
			if err != nil {
				return err
			}
			if of.Root == nil && of.Base == of.File {
				fmt.Printf("check %s\n", config.FormatKey(of.FileKey))
			} else {
				fmt.Printf("check %q %s\n", of.Path, config.FormatKey(of.File.Key()))
			}

			start := time.Now()
			var done mapset.Set[string]
			var uniq mapset.Set[string]
			var nfile, ndata, nlost, nerrs int

			// If this file came from a root pointer, and the root has an index,
			// verify that we can load the index data successfully.
			if of.Root != nil && of.Root.IndexKey != "" {
				_, err := config.LoadIndex(env.Context(), s.Files(), of.Root.IndexKey)
				if err != nil {
					fmt.Printf("* index %s: %v\n", config.FormatKey(of.Root.IndexKey), err)
					nerrs++
				} else {
					fmt.Printf("- index %s OK\n", config.FormatKey(of.Root.IndexKey))
				}
			}

			// Verify that all reachable files are loadable, and that their data
			// blocks exist in the store (without fetching them).
			if err := fpath.Walk(env.Context(), of.File, func(e fpath.Entry) error {
				if e.Err != nil {
					fmt.Printf("* error %q: %v\n", e.Path, e.Err)
					nerrs++
					return e.Err
				}

				// Count each occurrence of a file and its data blocks even if we've already seen it.
				nfile++
				want := mapset.New(e.File.Data().Keys()...)
				uniq.AddAll(want)
				ndata += e.File.Data().Len()

				// If (and only if) this is the first time we've seen this file,
				// make sure its data blocks are stored.
				if done.Has(e.File.Key()) {
					return nil // data blocks already checked
				}

				done.Add(e.File.Key())
				have, err := s.Files().Has(env.Context(), e.File.Data().Keys()...)
				if err != nil {
					fmt.Printf("* check data %q: %v", e.Path, err)
					nerrs++
					return nil
				}
				want.RemoveAll(have)
				if !want.IsEmpty() {
					for m := range want {
						fmt.Printf("* data missing %q %s\n", e.Path, config.FormatKey(m))
						nlost++
					}
				}
				return nil
			}); err != nil {
				return err
			}
			totalUnique := done.Len() + uniq.Len()
			if of.Root != nil {
				totalUnique++ // the root
				if of.Root.IndexKey != "" {
					totalUnique++ // the index
				}
			}
			fmt.Printf("%s: %d objects: %d files (%d unique), %d blocks (%d unique), %d lost, %d errors [%v elapsed]\n\n",
				value.Cond(nerrs == 0 && nlost == 0, "✅ OK", "❌ FAILED"),
				totalUnique, nfile, done.Len(), ndata, uniq.Len(), nlost, nerrs,
				time.Since(start).Round(time.Millisecond),
			)
		}
		return nil
	})
}

type statMod struct {
	perms        *int64
	ftype        *fs.FileMode
	modTime      *time.Time
	uid, gid     *int
	owner, group *string
	persist      *bool
	clear        bool
}

func parseStatMod(args []string) (*statMod, error) {
	var mod statMod

	i := 0
	for i < len(args) {
		if args[i] == "clear" {
			mod.clear = true
			i++
			continue
		}
		if i+1 >= len(args) {
			return nil, errors.New("odd-length argument list")
		}
		switch args[i] {
		case "mode":
			v, err := strconv.ParseInt(args[i+1], 0, 32)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.perms = &v

		case "type":
			var ftype fs.FileMode
			switch args[i+1] {
			case "f", "file":
				// OK, this is the default
			case "d", "dir":
				ftype |= fs.ModeDir
			case "l", "link", "symlink":
				ftype |= fs.ModeSymlink
			case "p", "pipe", "fifo":
				ftype |= fs.ModeNamedPipe
			case "s", "socket":
				ftype |= fs.ModeSocket
			case "b", "block", "bdev", "dev":
				ftype |= fs.ModeDevice
			case "c", "char", "cdev":
				ftype |= fs.ModeDevice | fs.ModeCharDevice
			default:
				return nil, fmt.Errorf("invalid type %q", args[i+1])
			}
			mod.ftype = &ftype

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
	return &mod, nil
}
