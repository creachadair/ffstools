//go:build all || badger

package cmdstorage

import "github.com/creachadair/badgerstore"

func init() { stores["badger"] = badgerstore.Opener }
