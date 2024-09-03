//go:build all || pebble

package cmdstorage

import "github.com/creachadair/pebblestore"

func init() { stores["pebble"] = pebblestore.Opener }
