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

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"

	// Subcommands.
	"github.com/creachadair/ffstools/ffs/internal/cmdblob"
	"github.com/creachadair/ffstools/ffs/internal/cmdexport"
	"github.com/creachadair/ffstools/ffs/internal/cmdfile"
	"github.com/creachadair/ffstools/ffs/internal/cmdgc"
	"github.com/creachadair/ffstools/ffs/internal/cmdmount"
	"github.com/creachadair/ffstools/ffs/internal/cmdput"
	"github.com/creachadair/ffstools/ffs/internal/cmdroot"
	"github.com/creachadair/ffstools/ffs/internal/cmdstatus"
	"github.com/creachadair/ffstools/ffs/internal/cmdsync"
)

var (
	configPath = config.Path()
	storeAddr  string
	debugLog   bool
)

func main() {
	root := &command.C{
		Name: command.ProgramName(),
		Usage: `<command> [arguments]
help [<command>]`,
		Help: `A command-line tool to manage FFS file trees.`,

		SetFlags: func(env *command.Env, fs *flag.FlagSet) {
			fs.StringVar(&configPath, "config", configPath, "Configuration file path")
			fs.StringVar(&storeAddr, "store", storeAddr, "Store service address (overrides config and environment)")
			fs.BoolVar(&debugLog, "debug", debugLog, "Enable debug logging (warning: noisy)")
		},

		Init: func(env *command.Env) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			if storeAddr != "" {
				cfg.DefaultStore = storeAddr
			} else if bs := os.Getenv("FFS_STORE"); bs != "" {
				cfg.DefaultStore = bs
			}
			if debugLog {
				cfg.EnableDebugLogging = true
			}
			config.ExpandString(&cfg.DefaultStore)
			env.Config = cfg
			return nil
		},

		Commands: []*command.C{
			cmdroot.Command,
			cmdfile.Command,
			cmdput.Command,
			cmdexport.Command,
			cmdsync.Command,
			cmdmount.Command,
			cmdgc.Command,
			cmdblob.Command,
			cmdstatus.Command,
			command.HelpCommand(nil),
			command.VersionCommand(),
		},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	command.RunOrFail(root.NewEnv(nil).SetContext(ctx).MergeFlags(true), os.Args[1:])
}
