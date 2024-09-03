//go:build all || bitcask

package cmdstorage

import "github.com/creachadair/bitcaskstore"

func init() { stores["bitcask"] = bitcaskstore.Opener }
