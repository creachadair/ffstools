//go:build all || sqlite

package cmdstorage

import "github.com/creachadair/sqlitestore"

func init() { stores["sqlite"] = sqlitestore.Opener }
