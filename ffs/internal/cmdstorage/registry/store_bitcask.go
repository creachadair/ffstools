//go:build all || bitcask

package registry

import "github.com/creachadair/bitcaskstore"

func init() { Stores["bitcask"] = bitcaskstore.Opener }
