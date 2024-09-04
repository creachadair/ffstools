//go:build all || pebble

package registry

import "github.com/creachadair/pebblestore"

func init() { Stores["pebble"] = pebblestore.Opener }
