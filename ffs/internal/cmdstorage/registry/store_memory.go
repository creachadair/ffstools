//go:build all || memory

package registry

import "github.com/creachadair/ffs/blob/memstore"

func init() { Stores["memory"] = memstore.Opener }
