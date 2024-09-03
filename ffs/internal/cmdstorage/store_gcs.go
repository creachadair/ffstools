//go:build all || gcs

package cmdstorage

import "github.com/creachadair/gcsstore"

func init() { stores["gcs"] = gcsstore.Opener }
