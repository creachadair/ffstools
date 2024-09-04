//go:build all || pogreb

package registry

import "github.com/creachadair/pogrebstore"

func init() { Stores["pogreb"] = pogrebstore.Opener }
