//go:build all || badger

package registry

import "github.com/creachadair/badgerstore"

func init() { Stores["badger"] = badgerstore.Opener }
