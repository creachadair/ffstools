//go:build all || buntdb

package registry

import "github.com/creachadair/buntdbstore"

func init() { Stores["buntdb"] = buntdbstore.Opener }
