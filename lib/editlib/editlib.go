// Package editlib handles a simple shell-like language of file modifications.
package editlib

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/creachadair/ffs/file"
)

// An Edit specifies a set of edits to apply to a [file.File].
type Edit struct {
	Perms        *uint32
	Mask         *uint32
	Type         *fs.FileMode
	ModTime      *time.Time
	UID, GID     *int
	Owner, Group *string
	DataSpec     *string
	Persist      *bool
	Clear        bool
	Create       bool
}

// ParseEdit parses an [Edit] specification from the given arguments.
func ParseEdit(args []string) (*Edit, error) {
	var mod Edit
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
			mod.DataSpec = &args[i+1]

		default:
			return nil, fmt.Errorf("unknown stat field %q", args[i])
		}
		i += 2
	}
	return &mod, nil
}

// Apply applies the specified edits to f. An error is reported if e fails to
// set the content of f; if e.DataSpec == nil, no error can occur.
//
// Note that the Create verb is not handled by Apply; f must not be nil.
// If e == nil, Apply makes no changes to f without error.
func (e *Edit) Apply(ctx context.Context, f *file.File) error {
	if e == nil {
		return nil
	}
	// Set file data.
	if e.DataSpec != nil {
		var derr error
		switch *e.DataSpec {
		case "":
			derr = f.SetData(ctx, strings.NewReader(""))
		case "-":
			derr = f.SetData(ctx, os.Stdin)
		default:
			df, err := os.Open(*e.DataSpec)
			if err != nil {
				return fmt.Errorf("open data: %w", err)
			}
			derr = f.SetData(ctx, df)
			df.Close()
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
func (e *Edit) ApplyRecursive(ctx context.Context, f *file.File) error {
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
