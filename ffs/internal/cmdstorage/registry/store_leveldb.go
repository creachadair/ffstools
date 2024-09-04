//go:build all || leveldb

package registry

import "github.com/creachadair/leveldbstore"

func init() { Stores["leveldb"] = leveldbstore.Opener }
