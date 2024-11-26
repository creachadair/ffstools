//go:build all || rosedb

package registry

import "github.com/creachadair/rosedbstore"

func init() { Stores["rosedb"] = rosedbstore.Opener }
