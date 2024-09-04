//go:build all || s3

package registry

import "github.com/creachadair/s3store"

func init() { Stores["s3"] = s3store.Opener }
