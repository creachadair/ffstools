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
)

// A Config specifies a set of edits to apply to a [file.File].
// A nil *Config is valid and applies no changes.
type Config struct {
	// If non-nil, permission bits to apply to the mode word.  If Mask == nil,
	// this value fully replaces the permissions; otherwise the masked bits of
	// Perms are applied to the corresponding bits of the mode word.
	Perms *uint32

	// If non-nil, specify which bits of the mode word will be affected by Perms.
	// This is ignored if Perms == nil.
	Mask *uint32

	// If non-nil, the file type to apply to the mode word.  Only the type
	// component is used, any other bits in the value are ignored (see Perms).
	Type *fs.FileMode

	// If non-nil, set the modification timestamp to this value.
	ModTime *time.Time

	// If non-nil, set the UID and GID to these values.
	// See also Owner and Group.
	UID, GID *int

	// If non-nil, set the Owner and Group names to these values.
	// See also UID and GID.
	Owner, Group *string

	// If non-nil, replace the contents of the target file with the contents of
	// this reader.
	Content io.Reader

	// If non-nil, set stat persistence for the file.
	Persist *bool

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
			mod.Perms = new(uint32(v))

		case "mask":
			v, err := strconv.ParseUint(args[i+1], 0, 32)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.Mask = new(uint32(v))

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
			mod.Type = &ftype

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
			mod.ModTime = &t

		case "uid", "gid":
			v, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			} else if args[i] == "uid" {
				mod.UID = &v
			} else {
				mod.GID = &v
			}

		case "owner":
			mod.Owner = &args[i+1]

		case "group":
			mod.Group = &args[i+1]

		case "persist":
			v, err := strconv.ParseBool(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
			mod.Persist = &v

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
	if e.Perms != nil {
		mask, perms := fs.ModePerm, *e.Perms
		if e.Mask != nil {
			mask = fs.FileMode(*e.Mask) // keep bits mentioned by the mask
			perms &= *e.Mask            // discard bits not mentioned by the mask
		}
		stat = stat.WithMode((stat.Mode &^ mask) | fs.FileMode(perms))
	}
	if e.Type != nil {
		stat = stat.WithMode((stat.Mode &^ fs.ModeType) | *e.Type)
	}
	if e.ModTime != nil {
		stat = stat.WithModTime(*e.ModTime)
	}
	if e.UID != nil {
		stat = stat.WithOwnerID(*e.UID)
	}
	if e.GID != nil {
		stat = stat.WithGroupID(*e.GID)
	}
	if e.Owner != nil {
		stat = stat.WithOwnerName(*e.Owner)
	}
	if e.Group != nil {
		stat = stat.WithGroupName(*e.Group)
	}
	if e.Persist != nil {
		stat.Persist(*e.Persist)
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
