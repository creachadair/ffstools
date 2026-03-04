//go:build linux

package cmdmount

import (
	"os"
	"time"

	"github.com/creachadair/mds/value"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var fuseOptions = fs.Options{
	MountOptions: fuse.MountOptions{
		// Because fusermount is setuid, the caller needs access both as root and
		// as the owning user to have permission to traverse across it.
		// This may require enabling user_allow_other in /etc/fuse.conf.
		// TODO(creachadair): Is there a better way to handle this?
		AllowOther: true,

		FsName: "ffs",
		Name:   "ffs",
	},
	EntryTimeout: value.Ptr(time.Second),
	AttrTimeout:  value.Ptr(time.Second),

	UID: uint32(max(os.Getuid(), 0)),
	GID: uint32(max(os.Getgid(), 0)),
}
