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

package cmdput

import (
	"flag"
	"fmt"
	"log"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/putlib"
)

var putConfig putlib.Config

var Command = &command.C{
	Name:  "put",
	Usage: "<path> ...",
	Help: `Write file and directory contents to the store.

Recursively copy each specified path from the local filesystem to the
store, and print the storage key. By default, file and directory stat
info are recorded; use -nostat to disable this. Use -xattr to capture
extended attributes.

Symbolic links are captured, but devices, sockets, FIFO, and other
special files are skipped.`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&putConfig.NoStat, "nostat", false, "Omit file and directory stat")
		fs.BoolVar(&putConfig.XAttr, "xattr", false, "Capture extended attributes")
		fs.BoolVar(&putConfig.Verbose, "v", false, "Enable verbose logging")
	},
	Run: runPut,
}

func runPut(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing required path")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		keys := make([]string, len(args))
		for i, path := range args {
			if putConfig.Verbose {
				log.Printf("put %q", path)
			}
			f, err := putConfig.PutPath(cfg.Context, s, path)
			if err != nil {
				return err
			}
			key, err := f.Flush(cfg.Context)
			if err != nil {
				return err
			}
			keys[i] = key
			if putConfig.Verbose {
				log.Printf("finished %q (%x)", path, key)
			}
		}
		for _, key := range keys {
			fmt.Printf("%x\n", key)
		}
		return nil
	})
}
