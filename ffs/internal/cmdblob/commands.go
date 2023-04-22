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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/prefixed"
	"github.com/creachadair/ffs/storage/suffixed"
	"github.com/creachadair/ffstools/ffs/config"
)

func getCmd(env *command.Env, args []string) error {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: get <key>...")
	}
	nctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(nctx)

	for _, arg := range args {
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

func sizeCmd(env *command.Env, args []string) error {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: size <key>...")
	}
	nctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(nctx)

	for _, arg := range args {
		key, err := config.ParseKey(arg)
		if err != nil {
			return err
		}
		data, err := bs.Get(nctx, key)
		if err != nil {
			return err
		}
		fmt.Println(hex.EncodeToString([]byte(key)), len(data))
	}
	return nil
}

func delCmd(env *command.Env, args []string) (err error) {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: delete <key>...")
	}
	nctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(nctx)

	missingOK := blobFlags.MissingOK
	for _, arg := range args {
		key, err := config.ParseKey(arg)
		if err != nil {
			return err
		}
		if err := bs.Delete(nctx, key); blob.IsKeyNotFound(err) && missingOK {
			continue
		} else if err != nil {
			return err
		}
		fmt.Println(hex.EncodeToString([]byte(key)))
	}
	return nil
}

func listCmd(env *command.Env, args []string) error {
	if len(args) != 0 {
		return errors.New("usage is: list")
	}
	start, err := config.ParseKey(blobFlags.Start)
	if err != nil {
		return err
	}
	pfx, err := config.ParseKey(blobFlags.Prefix)
	if err != nil {
		return err
	}
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)

	// If there is a prefix, apply it first since that will permit the
	// underlying scan to terminate sooner.
	if pfx != "" {
		p := prefixed.NewCAS(bs.Base()).Derive(pfx)
		bs.CAS = suffixed.NewCAS(p).Derive(blobFlags.Bucket)
	} else if blobFlags.Bucket != "" {
		bs.CAS = bs.CAS.Derive(blobFlags.Bucket)
	}

	var listed int
	return bs.List(ctx, start, func(key string) error {
		if blobFlags.Raw {
			fmt.Println(key)
		} else {
			fmt.Printf("%x\n", key)
		}
		listed++
		if blobFlags.MaxKeys > 0 && listed == blobFlags.MaxKeys {
			return blob.ErrStopListing
		}
		return nil
	})
}

func lenCmd(env *command.Env, args []string) error {
	if len(args) != 0 {
		return errors.New("usage is: len")
	}
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

func casPutCmd(env *command.Env, args []string) (err error) {
	ctx, cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer cas.Close(ctx)

	data, err := readData(ctx, "put", args)
	if err != nil {
		return err
	}
	key, err := cas.CASPut(ctx, data)
	if err != nil {
		return err
	}
	fmt.Printf("%x\n", key)
	return nil
}

func casKeyCmd(env *command.Env, args []string) error {
	ctx, cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer cas.Close(ctx)

	data, err := readData(ctx, "key", args)
	if err != nil {
		return err
	}
	key, err := cas.CASKey(ctx, data)
	if err != nil {
		return err
	}
	fmt.Printf("%x\n", key)
	return nil
}

func copyCmd(env *command.Env, args []string) error {
	if len(args) != 2 {
		return errors.New("usage is: copy <src> <dst>")
	}
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)

	srcKey, err := config.ParseKey(args[0])
	if err != nil {
		return err
	}
	dstKey, err := config.ParseKey(args[1])
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

func putCmd(env *command.Env, args []string) (err error) {
	if len(args) == 0 || len(args) > 2 {
		return errors.New("usage is: put <key> [<path>]")
	}
	key, err := config.ParseKey(args[0])
	if err != nil {
		return err
	}
	ctx, bs, err := storeFromEnv(env)
	if err != nil {
		return nil
	}
	defer bs.Close(ctx)

	data, err := readData(ctx, "put", args[1:])
	if err != nil {
		return err
	}

	return bs.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    data,
		Replace: blobFlags.Replace,
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

func storeFromEnv(env *command.Env) (context.Context, config.CAS, error) {
	t := env.Config.(*config.Settings)
	bs, err := t.OpenStore()

	// Becuase the blob commands operate on the raw store, take off the default
	// data bucket suffix (commands that want it can put it back on).
	bs.CAS = bs.CAS.Derive("")
	return t.Context, bs, err
}
