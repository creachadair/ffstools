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
	"os"
	"os/signal"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"

	// Subcommands.
	"github.com/creachadair/ffstools/ffs/internal/cmdblob"
	"github.com/creachadair/ffstools/ffs/internal/cmddebug"
	"github.com/creachadair/ffstools/ffs/internal/cmdexport"
	"github.com/creachadair/ffstools/ffs/internal/cmdfile"
	"github.com/creachadair/ffstools/ffs/internal/cmdgc"
	"github.com/creachadair/ffstools/ffs/internal/cmdmount"
	"github.com/creachadair/ffstools/ffs/internal/cmdput"
	"github.com/creachadair/ffstools/ffs/internal/cmdroot"
	"github.com/creachadair/ffstools/ffs/internal/cmdscan"
	"github.com/creachadair/ffstools/ffs/internal/cmdstatus"
	"github.com/creachadair/ffstools/ffs/internal/cmdstorage"
	"github.com/creachadair/ffstools/ffs/internal/cmdsync"
	"github.com/creachadair/ffstools/ffs/internal/cmdtar"
	"github.com/creachadair/ffstools/ffs/internal/cmdweb"
)

var flags = struct {
	ConfigPath    string        `flag:"config,default=*,Configuration file path"`
	StoreAddr     string        `flag:"store,default=$FFS_STORE,Store service address (socket path, host:port[+sub], or @name[+sub])"`
	ServicePrefix string        `flag:"service-prefix,default=$FFS_PREFIX,Store service method prefix"`
	SubstoreName  string        `flag:"substore,default=$FFS_SUBSTORE,Substore name"`
	DebugLog      bool          `flag:"debug,default=$FFS_DEBUG,Enable debug logging (warning: noisy)"`
	DialTimeout   time.Duration `flag:"dial-timeout,PRIVATE:Store service dial timeout"`
}{ConfigPath: config.Path()}

func main() {
	root := &command.C{
		Name: command.ProgramName(),
		Usage: `<command> [arguments]
help [<command>]`,
		Help: `A command-line tool to manage FFS file trees.`,

		SetFlags: command.Flags(flax.MustBind, &flags),

		Init: func(env *command.Env) error {
			cfg, err := config.Load(flags.ConfigPath)
			if err != nil {
				return err
			}
			if s := flags.StoreAddr; s != "" {
				cfg.DefaultStore = s
			}
			if s := flags.ServicePrefix; s != "" {
				cfg.ServicePrefix = s
			}
			if s := flags.SubstoreName; s != "" {
				cfg.Substore = s
			}
			if s := flags.DialTimeout; s > 0 {
				cfg.DialTimeout = config.Duration(s)
			}
			if flags.DebugLog {
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
			cmdtar.Command,
			cmdsync.Command,
			cmdscan.Command,
			cmdmount.Command,
			cmdgc.Command,
			cmdblob.Command,
			cmdstorage.Command,
			cmdweb.Command,
			cmdstatus.Command,
			cmddebug.Command,
			command.HelpCommand([]command.HelpTopic{{
				Name: "environment",
				Help: `Environment variables supported by ffs.

FFS_CONFIG     : Configuration file path (default: ` + config.DefaultPath + `)
FFS_DEBUG      : If true, enable debug logging (warning: noisy)
FFS_PASSPHRASE : If set, contains the passphrase for a --key file
FFS_PREFIX     : Storage service method name prefix (overrides config; overridden by --prefix)
FFS_STORE      : Storage service address (overrides config; overridden by --store)
FFS_SUBSTORE   : Substore name to use (overrides config; overridden by --substore)
`,
			}}),
			command.VersionCommand(),
		},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	command.RunOrFail(root.NewEnv(nil).SetContext(ctx), os.Args[1:])
}
