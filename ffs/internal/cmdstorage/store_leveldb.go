//go:build all || leveldb

package cmdstorage

import "github.com/creachadair/leveldbstore"

func init() { stores["leveldb"] = leveldbstore.Opener }
