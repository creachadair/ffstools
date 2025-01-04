// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
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

package cmdblob

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
)

func getCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		for _, arg := range env.Args {
			key, err := config.ParseKey(arg)
			if err != nil {
				return err
			}
			data, err := bs.Get(env.Context(), key)
			if err != nil {
				return err
			}
			os.Stdout.Write(data)
		}
		return nil
	})
}

func hasCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	var parsed []string
	for _, raw := range env.Args {
		key, err := config.ParseKey(raw)
		if err != nil {
			return err
		}
		parsed = append(parsed, key)
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		stat, err := bs.Has(env.Context(), parsed...)
		if err != nil {
			return err
		}
		has := stat.Slice()
		slices.Sort(has)
		for _, v := range has {
			fmt.Println(config.FormatKey(v))
		}
		return nil
	})
}

func delCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		missingOK := blobFlags.MissingOK
		dctx, cancel := context.WithCancel(env.Context())
		defer cancel()

		g, run := taskgroup.New(cancel).Limit(64)
		c := taskgroup.Gather(run, func(key string) {
			fmt.Println(config.FormatKey(key))
		})

		for _, arg := range env.Args {
			if dctx.Err() != nil {
				break
			}
			key, err := config.ParseKey(arg)
			if err != nil {
				return err
			}
			c.Report(func(report func(string)) error {
				if err := bs.Delete(dctx, key); blob.IsKeyNotFound(err) && missingOK {
					return nil
				} else if err != nil {
					return err
				}
				report(key)
				return nil
			})
		}
		return g.Wait()
	})
}

func listCmd(env *command.Env) error {
	start, err := config.ParseKey(listFlags.Start)
	if err != nil {
		return err
	}
	pfx, err := config.ParseKey(listFlags.Prefix)
	if err != nil {
		return err
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		var listed int
		return bs.List(env.Context(), start, func(key string) error {
			if !strings.HasPrefix(key, pfx) {
				return nil
			}
			if listFlags.Raw {
				fmt.Println(key)
			} else {
				fmt.Printf("%s\n", config.FormatKey(key))
			}
			listed++
			if listFlags.MaxKeys > 0 && listed == listFlags.MaxKeys {
				return blob.ErrStopListing
			}
			return nil
		})
	})
}

func lenCmd(env *command.Env) error {
	return withStoreFromEnv(env, func(bs blob.KV) error {
		n, err := bs.Len(env.Context())
		if err != nil {
			return err
		}
		fmt.Println(n)
		return nil
	})
}

func copyCmd(env *command.Env, srcArg, dstArg string) error {
	return withStoreFromEnv(env, func(bs blob.KV) error {
		srcKey, err := config.ParseKey(srcArg)
		if err != nil {
			return err
		}
		dstKey, err := config.ParseKey(dstArg)
		if err != nil {
			return err
		}
		src, err := bs.Get(env.Context(), srcKey)
		if err != nil {
			return err
		}
		return bs.Put(env.Context(), blob.PutOptions{
			Key:     dstKey,
			Data:    src,
			Replace: blobFlags.Replace,
		})
	})
}

func putCmd(env *command.Env, keyArg string, rest []string) (err error) {
	if len(rest) > 1 {
		return env.Usagef("extra arguments after path: %q", rest[1:])
	}
	key, err := config.ParseKey(keyArg)
	if err != nil {
		return err
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		data, err := readData(env.Context(), "put", rest)
		if err != nil {
			return err
		}

		return bs.Put(env.Context(), blob.PutOptions{
			Key:     key,
			Data:    data,
			Replace: blobFlags.Replace,
		})
	})
}

func syncKeysCmd(env *command.Env, keys []string) error {
	var parsed []string
	for _, key := range keys {
		p, err := config.ParseKey(key)
		if err != nil {
			return err
		}
		parsed = append(parsed, p)
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		need, err := blob.SyncKeys(env.Context(), bs, parsed)
		if err != nil {
			return err
		}
		for _, key := range need {
			fmt.Println(config.FormatKey(key))
		}
		return nil
	})
}

func readData(ctx context.Context, cmd string, args []string) (data []byte, err error) {
	if len(args) == 0 {
		data, err = io.ReadAll(os.Stdin)
	} else if len(args) == 1 {
		data, err = os.ReadFile(args[0])
	} else {
		return nil, fmt.Errorf("usage is: %s [<path>]", cmd)
	}
	return
}

func withStoreFromEnv(env *command.Env, f func(blob.KV) error) error {
	bs, err := env.Config.(*config.Settings).OpenStore(env.Context())
	if err != nil {
		return err
	}
	kv, err := bs.Store().KV(env.Context(), blobFlags.KV)
	if err != nil {
		return fmt.Errorf("open kv %q: %w", blobFlags.KV, err)
	}
	return f(kv)
}
