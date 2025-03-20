// Copyright 2025 Michael J. Fromberger. All Rights Reserved.
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

// Package cmdweb implements the "ffs web" subcommand.
package cmdweb

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
)

var Command = &command.C{
	Name: "web",
	Usage: `<address> <root-key>[/path]
<address> @<file-key>[/path]`,

	Help: `Export a read-only view of a filesystem via HTTP.

Run an HTTP server at the specified address that serves the
contents of an FFS file tree. The service runs until the program
is terminated by a signal.`,

	Run: command.Adapt(func(env *command.Env, address, rootKey string) error {
		cfg := env.Config.(*config.Settings)
		return cfg.WithStore(env.Context(), func(s config.Store) error {
			pi, err := config.OpenPath(env.Context(), s, rootKey)
			if err != nil {
				return err
			}
			fmt.Fprintf(env, "Resolved %q to %s\n", rootKey, config.FormatKey(pi.FileKey))

			fs := fpath.NewFS(env.Context(), pi.File)
			srv := &http.Server{
				Addr:    address,
				Handler: http.FileServerFS(fs),
			}
			go func() {
				<-env.Context().Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				srv.Shutdown(ctx)
			}()
			if strings.HasPrefix(address, ":") {
				address = "localhost" + address
			}
			fmt.Fprintf(env, "Serving at http://%s/\n", address)
			srv.ListenAndServe()
			fmt.Fprintln(env, "Server exited")
			return nil
		})
	}),
}
