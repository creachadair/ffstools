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
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
)

func getCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		for _, arg := range env.Args {
			key, err := filetree.ParseKey(arg)
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
		key, err := filetree.ParseKey(raw)
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

func sizeCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	var parsed []string
	for _, raw := range env.Args {
		key, err := filetree.ParseKey(raw)
		if err != nil {
			return err
		}
		parsed = append(parsed, key)
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		for _, key := range parsed {
			data, err := bs.Get(env.Context(), key)
			if errors.Is(err, blob.ErrKeyNotFound) {
				fmt.Print(config.FormatKey(key), "\tnot found\n")
			} else if err != nil {
				return err
			} else {
				fmt.Print(config.FormatKey(key), "\t", len(data), "\n")
			}
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
			key, err := filetree.ParseKey(arg)
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
	start, err := filetree.ParseKey(listFlags.Start)
	if err != nil {
		return err
	}
	pfx, err := filetree.ParseKey(listFlags.Prefix)
	if err != nil {
		return err
	}
	if start == "" && pfx != "" {
		start = pfx
	}
	return withStoreFromEnv(env, func(bs blob.KV) error {
		var listed int
		for key, err := range bs.List(env.Context(), start) {
			if err != nil {
				return err
			} else if !strings.HasPrefix(key, pfx) {
				if key > pfx {
					break
				}
				continue
			}
			if listFlags.Raw {
				fmt.Println(key)
			} else {
				fmt.Printf("%s\n", config.FormatKey(key))
			}
			listed++
			if listFlags.MaxKeys > 0 && listed == listFlags.MaxKeys {
				break
			}
		}
		return nil
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
		srcKey, err := filetree.ParseKey(srcArg)
		if err != nil {
			return err
		}
		dstKey, err := filetree.ParseKey(dstArg)
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
	key, err := filetree.ParseKey(keyArg)
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

func casPutCmd(env *command.Env) error {
	return withStoreFromEnv(env, func(bs blob.KV) error {
		data, err := readData(env.Context(), "cas-put", env.Args)
		if err != nil {
			return err
		}
		key, err := blob.CASFromKV(bs).CASPut(env.Context(), data)
		if err != nil {
			return err
		}
		fmt.Println(config.FormatKey(key))
		return nil
	})
}

func syncKeysCmd(env *command.Env, keys []string) error {
	var parsed []string
	for _, key := range keys {
		p, err := filetree.ParseKey(key)
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
		for key := range need {
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
	kv, err := bs.Base().KV(env.Context(), blobFlags.KV)
	if err != nil {
		return fmt.Errorf("open kv %q: %w", blobFlags.KV, err)
	}
	return f(kv)
}
