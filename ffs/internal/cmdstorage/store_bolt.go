//go:build all || bolt

package cmdstorage

import "github.com/creachadair/boltstore"

func init() { stores["bolt"] = boltstore.Opener }
