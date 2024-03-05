//go:build darwin

package cmdmount

import (
	"time"

	"github.com/creachadair/mds/value"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var fuseOptions = fs.Options{
	MountOptions: fuse.MountOptions{
		Options: []string{"volname=FFS", "noappledouble", "noubc"},
		FsName:  "ffs",
		Name:    "ffs",
	},
	EntryTimeout: value.Ptr(time.Second),
	AttrTimeout:  value.Ptr(time.Second),
}
