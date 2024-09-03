//go:build all || pogreb

package cmdstorage

import "github.com/creachadair/pogrebstore"

func init() { stores["pogreb"] = pogrebstore.Opener }
