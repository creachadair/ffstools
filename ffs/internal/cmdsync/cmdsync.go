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

// Package cmdsync implements the "ffs sync" subcommand.
package cmdsync

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/synclib"
	"github.com/creachadair/flax"
)

var syncFlags struct {
	Target       string `flag:"to,Target store (required unless --from is set)"`
	Source       string `flag:"from,Source store (required unless --to is set)"`
	Quiet        bool   `flag:"q,Reduce diagnostic output"`
	Verbose      bool   `flag:"v,Enable verbose logging"`
	VVerbose     bool   `flag:"vv,PRIVATE:Enable detailed verbose logging"`
	NoIndex      bool   `flag:"no-index,Do not use cached indices"`
	NoRoot       bool   `flag:"no-root,Do not copy referenced root pointers"`
	RootPrefix   string `flag:"root-prefix,Prefix target root names with this text"`
	RequireIndex bool   `flag:"require-index,Report an error if a specified root does not have an index"`
}

func progressLevel() int {
	if syncFlags.VVerbose {
		return synclib.ProgressDebug
	} else if syncFlags.Verbose {
		return synclib.ProgressDetail
	}
	return synclib.ProgressInfo
}

var Command = &command.C{
	Name: "sync",
	Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
	Help: `Synchronize file trees between stores.

Transfer all the objects reachable from the specified file or root paths
from one store into another. If --to=target is set, objects are copied
from --store to the specified target; if --from=source is set, objects are
copied from source to the --store. Exactly one of --to and --from must
be set.

By default, an argument that specifies a plain root name causes that root
(and its index, if one exists) to be copied along with the files reachable
from it. Use --no-root to skip copying the root and index, and only copy its
file tree.
`,

	SetFlags: command.Flags(flax.MustBind, &syncFlags),
	Run:      command.Adapt(runSync),
}

func qprintf(w io.Writer, msg string, args ...any) {
	if !syncFlags.Quiet {
		fmt.Fprintf(w, msg, args...)
	}
}

func runSync(env *command.Env, sourceKeys ...string) error {
	if (syncFlags.Target == "") == (syncFlags.Source == "") {
		return env.Usagef("exactly one of --from and --to must be set")
	}

	cfg := env.Config.(*config.Settings)
	otherSpec := cmp.Or(syncFlags.Target, syncFlags.Source)
	return cfg.WithStore(env.Context(), func(main filetree.Store) error {
		return cfg.WithStoreAddress(env.Context(), otherSpec, func(other filetree.Store) error {
			var src, tgt filetree.Store
			if syncFlags.Target != "" {
				qprintf(env, "Target store: %q\n", syncFlags.Target)
				src, tgt = main, other
			} else {
				qprintf(env, "Source store %q\n", syncFlags.Source)
				src, tgt = other, main
			}

			sc := synclib.Config{
				Source:       src,
				Target:       tgt,
				RequireIndex: syncFlags.RequireIndex,
				NoIndex:      syncFlags.NoIndex,
				SkipRoots:    syncFlags.NoRoot,
				Progress: func(_ int, msg string, args ...any) {
					fmt.Fprintf(env, msg, args...)
				},
				ProgressLevel: progressLevel(),
				BarWriter:     env,
			}
			if p := syncFlags.RootPrefix; p != "" {
				sc.RenameRoot = func(key string) string { return p + key }
			}

			ctx, cancel := context.WithCancel(env.Context())
			defer cancel()

			stats, err := sc.Sync(ctx, sourceKeys)
			if err != nil {
				return err
			}
			if stats.Copied != 0 || !syncFlags.Quiet {
				fmt.Fprintf(env, "Copied %d objects [%v elapsed]\n", stats.Copied, stats.Elapsed.Truncate(10*time.Millisecond))
			}
			return nil
		})
	})
}
