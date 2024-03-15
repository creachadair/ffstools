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

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
)

func getCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	nctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(nctx)

	for _, arg := range env.Args {
		key, err := config.ParseKey(arg)
		if err != nil {
			return err
		}
		data, err := bs.Get(nctx, key)
		if err != nil {
			return err
		}
		os.Stdout.Write(data)
	}
	return nil
}

func sizeCmd(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	nctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(nctx)

	for _, arg := range env.Args {
		key, err := config.ParseKey(arg)
		if err != nil {
			return err
		}
		data, err := bs.Get(nctx, key)
		if err != nil {
			return err
		}
		fmt.Println(config.FormatKey(key), len(data))
	}
	return nil
}

func delCmd(env *command.Env) (err error) {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <key>")
	}
	nctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(nctx)

	missingOK := blobFlags.MissingOK
	dctx, cancel := context.WithCancel(nctx)
	defer cancel()

	g, run := taskgroup.New(taskgroup.Trigger(cancel)).Limit(64)
	c := taskgroup.NewCollector(func(key string) {
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
		run(c.Stream(func(ch chan<- string) error {
			if err := bs.Delete(dctx, key); blob.IsKeyNotFound(err) && missingOK {
				return nil
			} else if err != nil {
				return err
			}
			ch <- key
			return nil
		}))
	}
	err = g.Wait()
	c.Wait()
	return err
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
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)

	if pfx != "" {
		bs.CAS = bs.CAS.WithPrefix(pfx)
	}

	var listed int
	return bs.List(ctx, start, func(key string) error {
		if listFlags.Raw {
			fmt.Println(pfx + key)
		} else {
			fmt.Printf("%s\n", config.FormatKey(pfx+key))
		}
		listed++
		if listFlags.MaxKeys > 0 && listed == listFlags.MaxKeys {
			return blob.ErrStopListing
		}
		return nil
	})
}

func lenCmd(env *command.Env) error {
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)

	n, err := bs.Len(ctx)
	if err != nil {
		return err
	}
	fmt.Println(n)
	return nil
}

func casPutCmd(env *command.Env) (err error) {
	ctx, cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer cas.Close(ctx)

	data, err := readData(ctx, "put", env.Args)
	if err != nil {
		return err
	}
	key, err := cas.CASPut(ctx, blob.CASPutOptions{Data: data})
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", config.FormatKey(key))
	return nil
}

func casKeyCmd(env *command.Env) error {
	ctx, cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer cas.Close(ctx)

	data, err := readData(ctx, "key", env.Args)
	if err != nil {
		return err
	}
	key, err := cas.CASKey(ctx, blob.CASPutOptions{Data: data})
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", config.FormatKey(key))
	return nil
}

func copyCmd(env *command.Env, srcArg, dstArg string) error {
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)

	srcKey, err := config.ParseKey(srcArg)
	if err != nil {
		return err
	}
	dstKey, err := config.ParseKey(dstArg)
	if err != nil {
		return err
	}
	src, err := bs.Get(ctx, srcKey)
	if err != nil {
		return err
	}
	return bs.Put(ctx, blob.PutOptions{
		Key:     dstKey,
		Data:    src,
		Replace: blobFlags.Replace,
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
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return nil
	}
	defer bs.Close(ctx)

	data, err := readData(ctx, "put", rest)
	if err != nil {
		return err
	}

	return bs.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    data,
		Replace: blobFlags.Replace,
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
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)

	need, err := bs.SyncKeys(ctx, parsed)
	if err != nil {
		return err
	}
	for _, key := range need {
		fmt.Println(config.FormatKey(key))
	}
	return nil
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

func storeFromEnv(env *command.Env) (context.Context, config.CAS, error) {
	t := env.Config.(*config.Settings)
	bs, err := t.OpenStore(env.Context())

	// Because the blob commands operate on the raw store, take off the default
	// data bucket suffix and apply the one from the -bucket flag.
	bs.CAS = bs.CAS.WithSuffix(blobFlags.Bucket)
	return env.Context(), bs, err
}
