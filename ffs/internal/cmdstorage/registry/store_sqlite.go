//go:build all || sqlite

package registry

import "github.com/creachadair/sqlitestore"

func init() { Stores["sqlite"] = sqlitestore.Opener }
