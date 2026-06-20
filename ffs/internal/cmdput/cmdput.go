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

// Package cmdput implements the "ffs put" subcommand.
package cmdput

import (
	"errors"
	"fmt"

	"github.com/creachadair/command"
	"github.com/creachadair/mds/shell"
)

var Command = &command.C{
	Name:        "put",
	Usage:       "<path> ...",
	Help:        `OBSOLETE: Use "ffs import" instead.`,
	CustomFlags: true,
	Unlisted:    true,
	Run: func(env *command.Env) error {
		alt := append([]string{"ffs", "import"}, env.Args...)
		fmt.Fprintf(env, `The "ffs put" subcommand is obsolete. Use instead:

  %s

`, shell.Join(alt))
		return errors.New("obsolete command")
	},
}
