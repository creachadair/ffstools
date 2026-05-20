//go:build linux

package cmdmount

import (
	"os"

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

	// TODO(creachadair): For some reason, populating entry and attribute
	// timeouts results in stat information being reported as zero values after
	// an initial read. This probably means there's something else I am doing
	// wrong, but in the meantime not setting these seems to mitigate it.

	UID: uint32(max(os.Getuid(), 0)),
	GID: uint32(max(os.Getgid(), 0)),
}
