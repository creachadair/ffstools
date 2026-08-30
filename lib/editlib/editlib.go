// Package editlib handles a simple shell-like language of file modifications.
package editlib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/creachadair/ffs/file"
	"github.com/creachadair/mds/value"
)

// A Config specifies a set of edits to apply to a [file.File].
// A nil *Config is valid and applies no changes.
type Config struct {
	// If present, permission bits to apply to the mode word.  If Mask is absent
	// this value fully replaces the permissions; otherwise the masked bits of
	// Perms are applied to the corresponding bits of the mode word.
	Perms value.Maybe[uint32]

	// If present, specify which bits of the mode word will be affected by
	// Perms.  This is ignored if Perms is absent.
	Mask value.Maybe[uint32]

	// If present, the file type to apply to the mode word.  Only the type
	// component is used, any other bits in the value are ignored (see Perms).
	Type value.Maybe[fs.FileMode]

	// If present, set the modification timestamp to this value.
	ModTime value.Maybe[time.Time]

	// If present, set the UID and GID to these values.
	// See also Owner and Group.
	UID, GID value.Maybe[int]

	// If present, set the Owner and Group names to these values.
	// See also UID and GID.
	Owner, Group value.Maybe[string]

	// If non-nil, replace the contents of the target file with the contents of
	// this reader.
	Content io.Reader

	// If present, set stat persistence for the file.
	Persist value.Maybe[bool]

	// If true, all existing stat information is cleared before any other edits
	// described in this config.
	Clear bool

	// If true, a new empty file should be created if it does not exist before
	// applying other edits described in this config. This field is not used by
	// the Apply method.
	Create bool

	closer io.Closer // set by ParseConfig
}

// ParseConfig parses a [Config] specification from the given arguments.
func ParseConfig(args []string) (*Config, error) {
	if len(args) == 0 {
		return nil, nil
	}
	var mod Config
	i := 0
	for i < len(args) {
		// Modifications that do not require an argument.
		switch args[i] {
		case "clear":
			mod.Clear = true
			i++
			continue
		case "create":
			mod.Create = true
			i++
			continue
		}

		// Modifications that require an argument.
		if i+1 >= len(args) {
			return nil, errors.New("odd-length argument list")
		}
		switch args[i] {
		case "mode", "perms":
			v, err := strconv.ParseUint(args[i+1], 0, 32)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.Perms = value.Just(uint32(v))

		case "mask":
			v, err := strconv.ParseUint(args[i+1], 0, 32)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.Mask = value.Just(uint32(v))

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
			mod.Type = value.Just(ftype)

		case "mtime", "modtime":
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
			mod.ModTime = value.Just(t)

		case "uid", "gid":
			v, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			} else if args[i] == "uid" {
				mod.UID = value.Just(v)
			} else {
				mod.GID = value.Just(v)
			}

		case "owner":
			mod.Owner = value.Just(args[i+1])

		case "group":
			mod.Group = value.Just(args[i+1])

		case "persist":
			v, err := strconv.ParseBool(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.Persist = value.Just(v)

		case "data", "content":
			switch args[i+1] {
			case "":
				mod.Content = bytes.NewReader(nil)
			case "-":
				mod.Content = os.Stdin
			default:
				f, err := os.Open(args[i+1])
				if err != nil {
					return nil, fmt.Errorf("invalid data file: %w", err)
				}
				mod.Content = f
				mod.closer = f
			}

		default:
			return nil, fmt.Errorf("unknown stat field %q", args[i])
		}
		i += 2
	}
	return &mod, nil
}

// Apply applies the specified edits to f. An error is reported if e fails to
// set the content of f; if e.Content == nil, no error can occur.
//
// Note that the Create verb is not handled by Apply; f must not be nil.
// If e == nil, Apply makes no changes to f without error.
func (e *Config) Apply(ctx context.Context, f *file.File) error {
	if e == nil {
		return nil
	}
	// Set file data.
	if e.Content != nil {
		derr := f.SetData(ctx, e.Content)
		if e.closer != nil {
			e.closer.Close()
		}
		if derr != nil {
			return fmt.Errorf("set data: %w", derr)
		}
	}

	// Set various stat fields.
	stat := f.Stat()
	if e.Clear {
		stat = stat.Clear()
	}
	if perms, ok := e.Perms.GetOK(); ok {
		mask := fs.ModePerm
		if m, ok := e.Mask.GetOK(); ok {
			mask = fs.FileMode(m) // keep bits mentioned by the mask
			perms &= m            // discard bits not mentioned by the mask
		}
		stat = stat.WithMode((stat.Mode &^ mask) | fs.FileMode(perms))
	}
	if t, ok := e.Type.GetOK(); ok {
		stat = stat.WithMode((stat.Mode &^ fs.ModeType) | t)
	}
	if m, ok := e.ModTime.GetOK(); ok {
		stat = stat.WithModTime(m)
	}
	if u, ok := e.UID.GetOK(); ok {
		stat = stat.WithOwnerID(u)
	}
	if g, ok := e.GID.GetOK(); ok {
		stat = stat.WithGroupID(g)
	}
	if o, ok := e.Owner.GetOK(); ok {
		stat = stat.WithOwnerName(o)
	}
	if g, ok := e.Group.GetOK(); ok {
		stat = stat.WithGroupName(g)
	}
	if p, ok := e.Persist.GetOK(); ok {
		stat.Persist(p)
	}
	stat.Update()
	return nil
}

// ApplyRecursive applies e recursively to all files reachable from f.
// On success, it flushes f to storage before returning.
func (e *Config) ApplyRecursive(ctx context.Context, f *file.File) error {
	if e == nil {
		return nil
	}
	if err := f.Scan(ctx, func(si file.ScanItem) error {
		return e.Apply(ctx, si.File)
	}); err != nil {
		return err
	}
	_, err := f.Flush(ctx)
	return err
}
