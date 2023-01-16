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

package cmdstatus

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/creachadair/chirpstore"
	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
)

var Command = &command.C{
	Name: "status",
	Help: "Print the status of the storage server.",

	Run: func(env *command.Env, args []string) error {
		if len(args) != 0 {
			return env.Usagef("extra arguments after command")
		}

		cfg := env.Config.(*config.Settings)
		return cfg.WithStore(cfg.Context, func(s config.CAS) error {
			cs, ok := s.Base().(chirpstore.CAS)
			if !ok {
				return errors.New("store does not support the status command")
			}
			data, err := cs.Status(cfg.Context)
			if err != nil {
				return err
			}
			fmt.Println(config.ToJSON(json.RawMessage(data)))
			return nil
		})
	},
}
