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
	"encoding/json"
	"errors"
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
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/editlib"
	"github.com/creachadair/ffstools/lib/scanlib"
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
			Name:  "hash",
			Usage: fileCmdUsage,
			Help:  "Print a cryptographic digest of file contents.",

			Run: runHash,
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
			Name: "edit",
			Usage: `<root-key>/<path> <stat-spec>
@<origin-key>/<path> <stat-spec>`,
			Help: `Edit stat and content of the specified path beneath the origin.

The edit spec is a list of fields to update, one or more of:

 mode <perms>   -- set file permissions (e.g., 0755)
 mask <perms>   -- file permissions mask (e.g., 0700, see below)
 type <type>    -- set file type (see below)
 mtime <time>   -- update file timestamp ("now", @<seconds>, or RFC3339)
 uid <id>       -- set the owner UID
 gid <id>       -- set the group GID
 owner <name>   -- set the owner name ("" to clear)
 group <name>   -- set the group name ("" to clear)
 persist <ok>   -- set or unset stat persistence
 data <src>     -- set the file content to <src> ("" to clear, "-" for stdin, or path)
 clear          -- clear all current stat values to zero (applies first)
 create         -- create the specified path if it does not exist (applies first)

By default, "mode" applies to the entire permissions word. With "mask", only
the bits mentioned by the mask are updated. For example:

  mode 050 mask 070   -- set only group permissions to r-x (g+rx)
  mode 0 mask 7       -- clear world permissions (o-rwx)
  mask 0444 mode 0777 -- set only r bits (ugo+r)

Bits in the existing mode word not mentioned by the mask are unmodified.
Bits in the new mode word not mentioned by the mask are ignored.

Allowed "type" values include:

 f, file:          regular file
 d, dir:           directory
 l, link, symlink: symbolic link
 p, pipe, fifo:    named pipe (FIFO)
 s, socket:        socket
 b, block, bdev:   block device
 c, char, cdev:    character device

If the origin is from a root, the root is updated with the changes.
As a special case, "create" allows "@" as the origin to create a new empty file.`,

			SetFlags: command.Flags(flax.MustBind, &editFlags),
			Run:      command.Adapt(runEdit),
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
			Name: "xattr",
			Usage: `<xattr-spec> <root-key>/<path>...
<xattr-spec> @<origin-key>/<path>...`,
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
			Name:     "list-paths",
			Usage:    "<root-key>/<path>\n@<origin-key>/<path>",
			Help:     "List all the paths recursively in a file tree.",
			SetFlags: command.Flags(flax.MustBind, &listPathsFlags),
			Run:      command.Adapt(runListPaths),
		},
		{
			Name:  "fsck",
			Usage: fileCmdUsage,
			Help: `Check file tree integrity.

For each specified origin, fsck recursively walks the corresponding file tree
and verifies that all the objects referenced by it exist in the underlying store.
If the origin includes a root pointer, and that root has an index, it also checks
that the index exists and that each referenced object is claimed by the index.

Any missing objects are logged, as well as any referenced objects not covered by
the index, if one is present.  After scanning, a summary is printed indicating
the numbers of objects found, categorized by type:

  objects: total number of objects of all types
  files:   number of file trees (total and unique)
  blocks:  number of data blobs (total and unique)
  lost:    number of distinct objects that could not be loaded
  errors:  number of errors of all kinds

With --data-size, it also computes the total size in bytes of all the data blocks,
both overall (including repeats) and unique (excluding repeats). Note that this
may greatly increase the time required to perform the scan, in a large file tree.

The scan succeeds if there were no errors, and no objects were lost.
Note that the totals presented will not be accurate if any errors occurred.

A failed scan indicates a corrupt or incomplete file tree. Index errors can often
be fixed by re-generating the index ("ffs root index"); missing objects must be
recovered or re-created separately.`,
			SetFlags: command.Flags(flax.MustBind, &fsckFlags),
			Run:      command.Adapt(runFileCheck),
		},
		{
			Name: "scan",
			Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
			Help:     `Scan blobs reachable from the specified file trees.`,
			SetFlags: command.Flags(flax.MustBind, &scanFlags),
			Run:      command.Adapt(runScan),
		},
		{
			Name: "index",
			Usage: `@<file-key>[/path/...] ...
<root-keyL[/path/...] ...`,
			Help: "Compute an index of blobs reachable from the specified file trees.",
			Run:  command.Adapt(runIndex),
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
			of, err := s.OpenPath(env.Context(), arg)
			if err != nil {
				return err
			}

			msg := file.Encode(of.File)
			if showFlags.Raw {
				bits, _ := wiretype.ToBinary(msg)
				os.Stdout.Write(bits)
			} else {
				fmt.Println(config.ToJSON(struct {
					S string `json:"storageKey"`
					N any    `json:"node"`
					H []byte `json:"dataHash,omitempty"`
				}{
					S: filetree.FormatKey32(of.FileKey),
					H: of.File.Data().Hash(),
					N: msg.Value.(*wiretype.Object_Node).Node,
				}))
			}
		}
		return nil
	})
}

func runHash(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, arg := range env.Args {
			if arg == "" {
				return env.Usagef("origin may not be empty")
			}
			of, err := s.OpenPath(env.Context(), arg)
			if err != nil {
				return err
			}
			fd := of.File.Data()
			fmt.Print(filetree.FormatKey64(string(fd.Hash())))
			if len(env.Args) > 1 {
				fmt.Printf("\t%s\n", arg)
			} else {
				fmt.Println()
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
			pi, err := s.OpenPath(env.Context(), arg)
			if err != nil {
				return err
			}
			of := pi.File
			name := path.Base(pi.Path)

			// List contents of directories unless -d is set.
			if (of.Child().Len() != 0 || of.Stat().Mode.IsDir()) && !listFlags.DirOnly {
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
			fmt.Print(config.DisplayKey(of.Key()), "\t")
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
		skey = config.DisplayKey(f.Key()) + "\t"
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
		Key    string            `json:"storageKey,omitempty"`
		XAttr  map[string][]byte `json:"xattr,omitempty"`
	}{
		Name: name,
		Type: tag, Mode: int64(s.Mode.Perm()), NLinks: 1 + f.Child().Len(),
		Owner: nameOrID(s.OwnerName, s.OwnerID), Group: nameOrID(s.GroupName, s.GroupID),
		Size: f.Data().Size(), Blocks: f.Data().Len(), MTime: s.ModTime.UTC(),
		Target: target, Key: filetree.FormatKey32(f.Key()), XAttr: xattr,
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
		of, err := s.OpenPath(env.Context(), originPath)
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
		tf, err := s.OpenPath(env.Context(), target)
		if err != nil {
			return err
		}
		key, err := s.SetPath(env.Context(), originPath, tf.File)
		if err != nil {
			return err
		}
		fmt.Printf("set: %s\n", filetree.FormatKey32(key))
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
				return fmt.Errorf(`missing path %q (use "root delete" to delete a root)`, arg)
			}
			of, err := s.OpenPath(env.Context(), base) // N.B. No path; see below
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
			fmt.Printf("remove: %s\n", filetree.FormatKey32(key))
		}
		return nil
	})
}

var editFlags struct {
	Recur bool `flag:"recur,Apply edits recursively (warning: risky)"`
}

func runEdit(env *command.Env, pathSpec string, mods []string) error {
	if len(mods) == 0 {
		return env.Usagef("missing edit spec")
	}
	mod, err := editlib.ParseEdit(mods)
	if err != nil {
		return fmt.Errorf("invalid edit spec: %w", err)
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		// If mod.create is set, treat "@" as a flag to create a new empty file.
		if pathSpec == "@" && mod.Create {
			key, err := file.New(s.Files(), &file.NewOptions{PersistStat: true}).Flush(env.Context())
			if err != nil {
				return err
			}
			pathSpec += filetree.FormatKey32(key)
		}

		tf, err := s.OpenPath(env.Context(), pathSpec)
		if errors.Is(err, file.ErrChildNotFound) && mod.Create {
			// The specified path does not exist, but "create" is set, so do that,
			// then try to reopen the path.
			var pk string
			pk, err = s.SetPath(env.Context(), pathSpec, file.New(s.Files(), &file.NewOptions{
				Name:        path.Base(pathSpec),
				PersistStat: true,
			}))
			if err == nil {
				// If the original spec was a file tree origin, we have to update the path.
				if first, rest := filetree.SplitPath(pathSpec); strings.HasPrefix(first, "@") {
					pathSpec = path.Join("@"+filetree.FormatKey32(pk), rest)
				}
				tf, err = s.OpenPath(env.Context(), pathSpec)
			}
		}
		if err != nil {
			return err
		}

		if editFlags.Recur {
			err = mod.ApplyRecursive(env.Context(), tf.File)
		} else {
			err = mod.Apply(env.Context(), tf.File)
		}
		if err != nil {
			return err
		}
		key, err := tf.Flush(env.Context())
		if err != nil {
			return err
		}
		fmt.Printf("edit: %s\n", filetree.FormatKey32(key))
		return nil
	})
}

func runXAttr(env *command.Env, op string, argsAndSpecs ...string) error {
	args, specs, err := parseXAttrSpec(op, argsAndSpecs)
	if err != nil {
		return err
	} else if len(specs) == 0 {
		return env.Usagef("at least one origin/path is required")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, fileSpec := range specs {
			of, err := s.OpenPath(env.Context(), fileSpec)
			if err != nil {
				return err
			}
			oldKey := of.BaseKey
			xv := of.File.XAttr()

			var modified bool
			switch op {
			case "list":
				if xv.Len() != 0 {
					fmt.Println(strings.Join(xv.Names(), "\n"))
				}
			case "clear":
				if n := xv.Len(); n != 0 {
					xv.Clear()
					modified = true
					fmt.Fprintf(env, "removed %d xattr\n", n)
				}
			case "get":
				if xv.Has(args[0]) {
					fmt.Println(xv.Get(args[0]))
				} else {
					fmt.Fprintf(env, "xattr %q not found\n", args[0])
				}
			case "delete":
				if xv.Has(args[0]) {
					xv.Remove(args[0])
					modified = true
					fmt.Fprintf(env, "removed xattr %q\n", args[0])
				}
			case "set":
				xv.Set(args[0], args[1])
				modified = true
			default:
				panic("unknown xattr spec: " + op) // unreachable
			}

			if modified {
				key, err := of.Flush(env.Context())
				if err != nil {
					return err
				}
				if key != oldKey {
					fmt.Printf("xattr: %s\n", filetree.FormatKey32(key))
				}
			}
		}
		return nil
	})
}

func parseXAttrSpec(op string, args []string) (opArgs, specs []string, _ error) {
	switch op {
	case "list", "clear":
		return nil, args, nil // all operands are specs
	case "get", "delete":
		if len(args) == 0 {
			return nil, nil, fmt.Errorf("missing required operand for %q", op)
		}
		return args[:1], args[1:], nil
	case "set":
		if len(args) < 2 {
			return nil, nil, fmt.Errorf("wrong number of args for %q (got %d, want 2)", op, len(args))
		}
		return args[:2], args[2:], nil
	default:
		return nil, nil, fmt.Errorf("unknown xattr operation %q", op)
	}
}

var resolveFlags struct {
	Path bool `flag:"path,Show each key traversed by the path"`
}

func runResolve(env *command.Env, originPath string) error {
	cfg := env.Config.(*config.Settings)
	if !resolveFlags.Path {
		return cfg.WithStore(env.Context(), func(s filetree.Store) error {
			rf, err := s.OpenPath(env.Context(), env.Args[0])
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", filetree.FormatKey32(rf.File.Key()))
			return nil
		})
	}
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		base, rest := filetree.SplitPath(originPath)
		rf, err := s.OpenPath(env.Context(), base) // N.B. No path; see below
		if err != nil {
			return err
		}
		if rf.RootKey != "" {
			fmt.Printf("%s %s\n", filetree.FormatKey32(rf.Base.Key()), rf.RootKey)
		} else {
			fmt.Printf("%s\n", filetree.FormatKey32(rf.Base.Key()))
		}
		parts := strings.Split(rest, "/")
		pf, err := fpath.OpenPath(env.Context(), rf.Base, rest)
		for i, f := range pf {
			fmt.Printf("%s %s\n", filetree.FormatKey32(f.Key()), parts[i])
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
		of, err := s.OpenPath(env.Context(), origin)
		if err != nil {
			return err
		}
		want := mapset.New(parsed...)
		werr := fpath.Walk(env.Context(), of.File, func(e fpath.Entry) error {
			if e.Err != nil {
				return err
			}
			if want.Has(e.File.Key()) {
				fmt.Printf("file %q %s\n", e.Path, filetree.FormatKey32(e.File.Key()))
				if !findFlags.All {
					return errFindFound
				}
			}
			for i, dkey := range e.File.Data().Keys() {
				if want.Has(dkey) {
					fmt.Printf("data %q [%d] %s\n", e.Path, i, filetree.FormatKey32(dkey))
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

var listPathsFlags struct {
	Full bool `flag:"full,List full paths including the origin"`
	Key  bool `flag:"key,Include storage keys"`
}

func runListPaths(env *command.Env, pathSpec string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := s.OpenPath(env.Context(), pathSpec)
		if err != nil {
			return err
		}
		return fpath.Walk(env.Context(), of.File, func(e fpath.Entry) error {
			if e.Err != nil {
				return e.Err
			}
			if listPathsFlags.Key {
				fmt.Print(config.DisplayKey(e.File.Key()), "\t")
			}
			if listPathsFlags.Full {
				fmt.Println(path.Join(of.Path, e.Path))
			} else if e.Path != "" || listPathsFlags.Key {
				// Don't print the empty root path unless we are also printing a key.
				fmt.Println(e.Path)
			}
			return nil
		})
	})
}

var fsckFlags struct {
	DataSize bool `flag:"data-size,Compute the aggregate sizes of data blocks (WARNING: expensive)"`
}

func runFileCheck(env *command.Env, origins ...string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, org := range origins {
			of, err := s.OpenPath(env.Context(), org)
			if err != nil {
				return err
			}
			if of.Root == nil && of.Base == of.File {
				fmt.Printf("check %s\n", filetree.FormatKey32(of.FileKey))
			} else {
				fmt.Printf("check %q %s\n", of.Path, filetree.FormatKey32(of.File.Key()))
			}

			start := time.Now()
			dataSize := make(map[string]int64)
			var done, uniq, lost mapset.Set[string]
			var nfile, ndata, nerrs int
			var totalDataBytes int64

			// If this file came from a root pointer, and the root has an index,
			// verify that we can load the index data successfully.
			//
			// If we do have an index, we will also verify that all the reachable
			// file and data blobs are recorded there.
			checkIndex := func(string) bool { return true } // fail open
			if of.Root == nil {
				// no root is invoolved
			} else if of.Root.IndexKey == "" {
				fmt.Printf("- root %q is not indexed (OK)\n", of.RootKey)
			} else if idx, err := s.LoadIndex(env.Context(), of.Root.IndexKey); err != nil {
				fmt.Printf("* index %s: %v\n", filetree.FormatKey32(of.Root.IndexKey), err)
				lost.Add(of.Root.IndexKey)
				nerrs++
			} else {
				st := idx.Stats()
				fmt.Printf("▷ index %s OK (%d keys, %d bits, %d hashes)\n", filetree.FormatKey32(of.Root.IndexKey),
					st.NumKeys, st.FilterBits, st.NumHashes)
				uniq.Add(of.Root.IndexKey) // lives in the content-addressed store
				checkIndex = idx.Has
			}

			// Verify that all reachable files are loadable, and that their data
			// blocks exist in the store (without fetching them).
			if err := fpath.Walk(env.Context(), of.File, func(e fpath.Entry) error {
				if e.Err != nil {
					msg := e.Err.Error()
					if e, ok := errors.AsType[*blob.KeyError](e.Err); ok {
						msg = filetree.FormatKey32(e.Key)
						lost.Add(e.Key)
					}
					fmt.Printf("* file missing %s %q\n", msg, e.Path)
					nerrs++
					return fpath.ErrSkipChildren // continue scanning
				}

				fkey := e.File.Key()
				if !checkIndex(fkey) {
					fmt.Printf("* index: missing file %s\n", filetree.FormatKey32(fkey))
					nerrs++
				}

				// Count each occurrence of a file and its data blocks even if we've already seen it.
				nfile++
				fd := e.File.Data()

				want := mapset.New(fd.Keys()...)
				uniq.AddAll(want)
				ndata += fd.Len()

				// If we are asked to compute data size, read each data blob we
				// have not seen before and cache its size.
				if fsckFlags.DataSize {
					want.RemoveAll(lost) // exclude keys already known to be lost (we already reported them)
					for dk := range want {
						sz, ok := dataSize[dk]
						if !ok {
							bits, err := s.Files().Get(env.Context(), dk)
							if errors.Is(err, context.Canceled) {
								return err
							} else if err != nil {
								fmt.Printf("* data missing %s %q\n", filetree.FormatKey32(dk), e.Path)
								lost.Add(dk)
								continue
							}
							sz = int64(len(bits))
							dataSize[dk] = sz
						}
						totalDataBytes += sz
					}
				}

				// If (and only if) this is the first time we've seen this file,
				// make sure its data blocks are stored.
				if done.Has(e.File.Key()) {
					return nil // data blocks already checked
				}

				done.Add(fkey)
				for dk := range want {
					if !checkIndex(dk) {
						fmt.Printf("* index: missing data %s\n", filetree.FormatKey32(dk))
						nerrs++
					}
				}

				// Check that all the data block keys are at least nominally
				// present in the store.  We don't need to do this if --data-size
				// is set, however, because we've already fetched them all in that
				// case in order to determine their size.
				if !fsckFlags.DataSize {
					have, err := s.Files().Has(env.Context(), fd.Keys()...)
					if err != nil {
						fmt.Printf("* check data %q: %v", e.Path, err)
						nerrs++
						return nil
					}
					want.RemoveAll(have)
					if !want.IsEmpty() {
						for m := range want {
							fmt.Printf("* data missing %s %q\n", filetree.FormatKey32(m), e.Path)
							lost.Add(m)
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
			var totalUniqueDataBytes int64
			for _, v := range dataSize {
				totalUniqueDataBytes += v
			}
			totalUnique := done.Len() + uniq.Len() // N.B. unique includes the index, if there was one
			if fsckFlags.DataSize {
				fmt.Printf("▷ total data size: %s, %s (%.1f%%)\n",
					formatBytes(totalDataBytes, "bytes"), formatBytes(totalUniqueDataBytes, "unique"),
					percent(totalUniqueDataBytes, totalDataBytes))
			}
			fmt.Printf("%s: %d objects: %d files (%d unique, %.1f%%), %d blocks (%d unique, %.1f%%), "+
				"%d lost, %d errors\n",
				value.Cond(nerrs == 0 && lost.Len() == 0, "✅ OK", "❌ FAILED"),
				totalUnique, nfile, done.Len(), percent(done.Len(), nfile),
				ndata, uniq.Len(), percent(uniq.Len(), ndata), lost.Len(), nerrs)
			fmt.Printf("🕖 %v elapsed\n\n", time.Since(start).Round(time.Millisecond))
		}
		return nil
	})
}

var scanFlags struct {
	Keys bool `flag:"keys,Print keys to stdout"`
	Type bool `flag:"type,Print the type of each key (with --keys)"`
}

func runScan(env *command.Env, sourceKeys ...string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(src filetree.Store) error {
		// Find all the objects reachable from the specified starting points.
		worklist := scanlib.NewScanner(src.Files())
		scanStart := time.Now()
		for _, elt := range sourceKeys {
			of, err := src.OpenPath(env.Context(), elt)
			if err != nil {
				return err
			}

			if of.Root != nil && of.Base == of.File {
				fmt.Fprintf(env, "Scanning data reachable from root %q...\n", of.RootKey)
				err = worklist.ScanRoot(env.Context(), of.RootKey, of.Root)
			} else {
				fmt.Fprintf(env, "Scanning data reachable from file %s ...\n", filetree.FormatKey32(of.FileKey))
				err = worklist.ScanFile(env.Context(), of.File)
			}
			if err != nil {
				return err
			}
		}
		stats := worklist.Stats()
		fmt.Fprintf(env, "Found %d reachable objects (%d roots, %d files, %d blobs) [%v elapsed]\n",
			worklist.Len(), stats.NumRoots, stats.NumFiles, stats.NumBlobs, time.Since(scanStart).Round(time.Millisecond))

		if scanFlags.Keys {
			for chunk := range worklist.Chunks(256) {
				for _, key := range chunk {
					if scanFlags.Type {
						fmt.Printf("%c %s\n", worklist.Type(key), filetree.FormatKey32(key))
					} else {
						fmt.Println(filetree.FormatKey32(key))
					}
				}
			}
		}
		return nil
	})
}

func runIndex(env *command.Env, sourceKeys ...string) error {
	if len(sourceKeys) == 0 {
		return env.Usagef("no source keys specified")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(src filetree.Store) error {
		start := time.Now()

		n, err := src.Files().Len(env.Context())
		if err != nil {
			return fmt.Errorf("calculate filter length: %w", err)
		}
		idx := index.New(int(n), &index.Options{FalsePositiveRate: 0.005})

		var files, data blob.KeySet
		for _, spec := range sourceKeys {
			of, err := src.OpenPath(env.Context(), spec)
			if err != nil {
				return err
			}
			if err := of.File.Scan(env.Context(), func(si file.ScanItem) error {
				key := si.Key()
				if files.Has(key) {
					return file.ErrSkipChildren // don't re-scan repeats of the same file
				}
				files.Add(key)
				idx.Add(key)
				for _, dk := range si.Data().Keys() {
					if !data.Has(dk) {
						idx.Add(dk)
						data.Add(dk)
					}
				}
				return nil
			}); err != nil {
				return fmt.Errorf("scanning %s: %w", filetree.FormatKey32(of.File.Key()), err)
			}
		}
		fmt.Fprintf(env, "Finished scanning %d objects [%v elapsed]\n",
			idx.Len(), time.Since(start).Truncate(10*time.Millisecond))

		ikey, err := src.SaveIndex(env.Context(), idx)
		if err != nil {
			return fmt.Errorf("saving index: %w", err)
		}
		fmt.Println(filetree.FormatKey32(ikey))
		return nil
	})
}

func formatBytes(n int64, label string) string {
	if n < 1<<10 {
		return fmt.Sprintf("%d %s", n, label)
	}
	const unit = "KMGTPEZY" // lol
	i, fn := -1, float64(n)
	for fn > 1024 {
		fn /= 1024
		i++
	}
	return fmt.Sprintf("%d %s [%.1f%siB]", n, label, fn, unit[i:i+1])
}

func percent[N ~int | ~int64](v, total N) float64 { return 100 * (float64(v) / float64(total)) }
