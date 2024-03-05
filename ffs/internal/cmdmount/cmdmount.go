// Copyright 2024 Michael J. Fromberger. All Rights Reserved.
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

package cmdmount

import (
	"flag"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffuse/driver"
)

var svc = &driver.Service{Options: fuseOptions}

var Command = &command.C{
	Name:  "mount",
	Usage: "<root-key>[/path] <mount-path> [cmd args...]\n@<file-key>[/path] <mount-path> [cmd args...]",
	Help:  "Mount a file tree as a FUSE filesystem.",

	SetFlags: func(env *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&svc.ReadOnly, "read-only", false, "Mount the filesystem as read-only")
		fs.IntVar(&svc.DebugLog, "dlog", 0, "Set debug logging level (1=ffs, 2=fuse, 3=both)")
		fs.DurationVar(&svc.AutoFlush, "auto-flush", 0, "Automatically flush the root at this interval")
		fs.BoolVar(&svc.Verbose, "v", false, "Enable verbose logging")
		fs.BoolVar(&svc.Exec, "exec", false, "Execute a command, then unmount and exit")
	},

	Run: command.Adapt(func(env *command.Env, rootKey, mountPath string, cmdArgs ...string) error {
		cfg := env.Config.(*config.Settings)
		return cfg.WithStore(env.Context(), func(s config.CAS) error {
			svc.MountPath = mountPath
			svc.RootKey = rootKey
			svc.Store = s
			svc.ExecArgs = cmdArgs
			if err := svc.Init(env.Context()); err != nil {
				return err
			}
			return svc.Run(env.Context())
		})
	}),
}
