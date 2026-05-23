// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package config

import "golang.org/x/sys/unix"

// isDescriptorValid reports whether the specified file descriptor is valid.
func isDescriptorValid(fd uintptr) bool {
	v, err := unix.FcntlInt(fd, unix.F_GETFD, 0)
	return err == nil && v >= 0
}
