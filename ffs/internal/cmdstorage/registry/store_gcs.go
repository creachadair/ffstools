//go:build all || gcs

package registry

import "github.com/creachadair/gcsstore"

func init() { Stores["gcs"] = gcsstore.Opener }
