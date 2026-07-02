// Copyright 2022 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importlib

import (
	"io/fs"
	"os"

	"github.com/pkg/xattr"
)

var (
	_ fs.FS         = osFS{}
	_ fs.ReadDirFS  = osFS{}
	_ fs.ReadLinkFS = osFS{}
	_ XAttrFS       = osFS{}
)

// osFS is an implementation of [fs.FS] and related interfaces using the
// standard functions from the [os] package.
//
// Unlike [os.DirFS] or [os.Root], this implementation does not constrain paths
// to a particular view of the file tree.
type osFS struct{}

func (osFS) Open(path string) (fs.File, error)          { return os.Open(path) }
func (osFS) Stat(path string) (fs.FileInfo, error)      { return os.Stat(path) }
func (osFS) ReadLink(path string) (string, error)       { return os.Readlink(path) }
func (osFS) Lstat(path string) (fs.FileInfo, error)     { return os.Lstat(path) }
func (osFS) ReadDir(path string) ([]fs.DirEntry, error) { return os.ReadDir(path) }
func (osFS) ListXAttr(path string) ([]string, error)    { return xattr.LList(path) }
func (osFS) GetXAttr(path, name string) ([]byte, error) { return xattr.LGet(path, name) }

// XAttrFS is an optional extension interface for [fs.FS] that includes support
// for reading extended attributes.
type XAttrFS interface {
	fs.FS

	// ListXAttr reports the names of all the extended attributes on path.
	ListXAttr(path string) ([]string, error)

	// GetXAttr returns the complete contents of the specified extended
	// attribute on path. If no such attribute exists, it must return an error
	// matching [fs.ErrNotExist].
	GetXAttr(path, name string) ([]byte, error)
}
