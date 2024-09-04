//go:build all || bolt

package registry

import "github.com/creachadair/boltstore"

func init() { Stores["bolt"] = boltstore.Opener }
