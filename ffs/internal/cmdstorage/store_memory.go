//go:build all || memory

package cmdstorage

import "github.com/creachadair/ffs/blob/memstore"

func init() { stores["memory"] = memstore.Opener }
