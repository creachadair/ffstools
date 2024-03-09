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
	"fmt"
	"log"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffuse/driver"
)

var svc = &driver.Service{Options: fuseOptions}

var Command = &command.C{
	Name: "mount",
	Usage: `<root-key>[/path] <mount-path> [--exec cmd args...]
@<file-key>[/path] <mount-path> [--exec cmd args...]`,

	Help: `Mount a file tree as a FUSE filesystem.

Mount a FUSE filesystem at the specified <mount-path> hosting the contents
of the specified FFS file. By default, the mounted filesystem is writable.
With --read-only, the filesystem is instead mounted as read-only.

If the file is based on a root key, then changes made while the filesystem
is mounted are flushed back to that root upon exit. If the --auto-flush flag
is set, changes are additionally flushed at the specified interval.

By default, the filesystem runs until the program is terminated by a signal
or the filesystem is unmounted (e.g., by calling umount).  With --exec, the
specified command and arguments are run as a subprocess with their initial
working directory set to the root of the mounted filesystem. In this mode,
the filesystem is automatically unmounted when the subprocess exits.
`,

	SetFlags: func(env *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&svc.ReadOnly, "read-only", false, "Mount the filesystem as read-only")
		fs.BoolVar(&svc.DebugLog, "debug-fuse", false, "PRIVATE:Enable FUSE debug logging (warning: prolific)")
		fs.DurationVar(&svc.AutoFlush, "auto-flush", 0, "Automatically flush the root at this interval")
		fs.BoolVar(&svc.Verbose, "v", false, "Enable verbose logging")
		fs.BoolVar(&svc.Exec, "exec", false, "Execute a command, then unmount and exit")
	},

	Run: command.Adapt(func(env *command.Env, rootKey, mountPath string, cmdArgs ...string) error {
		if !svc.Exec && len(cmdArgs) != 0 {
			return env.Usagef("extra arguments after command: %q", cmdArgs)
		}
		cfg := env.Config.(*config.Settings)
		return cfg.WithStore(env.Context(), func(s config.CAS) error {
			svc.MountPath = mountPath
			svc.RootKey = rootKey
			svc.Store = s
			svc.ExecArgs = cmdArgs
			ctx := env.Context()
			if err := svc.Init(ctx); err != nil {
				return err
			}
			// If the filesystem is read-only, we can run without follow-up.
			if svc.ReadOnly {
				return svc.Run(ctx)
			}

			// Otherwise, we need to persist the root once the filesystem exits.
			// If the filesystem failed, don't overwrite the root with changes,
			// but do give the user feedback about the latest state.
			if err := svc.Run(ctx); err != nil {
				if key, err := svc.Path.Base.Flush(ctx); err == nil {
					fmt.Printf("state: %s\n", config.FormatKey(key))
				} else {
					log.Printf("WARNING: Flushing file state failed: %v", err)
				}
				return fmt.Errorf("filesystem failed: %w", err)
			}

			// Success: Write changed data back out, if any.
			rk, err := svc.Path.Flush(ctx)
			if err != nil {
				return fmt.Errorf("flush file data: %w", err)
			}
			fmt.Printf("%s\n", config.FormatKey(rk))
			return nil
		})
	}),
}
