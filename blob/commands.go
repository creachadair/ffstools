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

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/prefixed"
)

func getContext(env *command.Env) context.Context {
	return env.Config.(*settings).Context
}

func getCmd(env *command.Env, args []string) error {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: get <key>...")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	nctx := getContext(env)
	defer blob.CloseStore(nctx, bs)

	for _, arg := range args {
		key, err := parseKey(arg)
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
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	nctx := getContext(env)
	defer blob.CloseStore(nctx, bs)

	for _, arg := range args {
		key, err := parseKey(arg)
		if err != nil {
			return err
		}
		size, err := bs.Size(nctx, key)
		if err != nil {
			return err
		}
		fmt.Println(hex.EncodeToString([]byte(key)), size)
	}
	return nil
}

func delCmd(env *command.Env, args []string) (err error) {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: delete <key>...")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	nctx := getContext(env)
	defer blob.CloseStore(nctx, bs)

	missingOK := env.Config.(*settings).MissingOK
	for _, arg := range args {
		key, err := parseKey(arg)
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
	cfg := env.Config.(*settings)
	start, err := parseKey(cfg.Start)
	if err != nil {
		return err
	}
	pfx, err := parseKey(cfg.Prefix)
	if err != nil {
		return err
	}
	if pfx != "" && start == "" {
		start = pfx
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, bs)

	var listed int
	return bs.List(ctx, start, func(key string) error {
		if !strings.HasPrefix(key, pfx) {
			if key > pfx {
				return blob.ErrStopListing
			}
			return nil
		}
		if cfg.Raw {
			fmt.Println(key)
		} else {
			fmt.Printf("%x\n", key)
		}
		listed++
		if cfg.MaxKeys > 0 && listed == cfg.MaxKeys {
			return blob.ErrStopListing
		}
		return nil
	})
}

func lenCmd(env *command.Env, args []string) error {
	if len(args) != 0 {
		return errors.New("usage is: len")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, bs)

	n, err := bs.Len(ctx)
	if err != nil {
		return err
	}
	fmt.Println(n)
	return nil
}

func casPutCmd(env *command.Env, args []string) (err error) {
	cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, cas)

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
	cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, cas)

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
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, bs)

	srcKey, err := parseKey(args[0])
	if err != nil {
		return err
	}
	dstKey, err := parseKey(args[1])
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
		Replace: env.Config.(*settings).Replace,
	})
}

func statCmd(env *command.Env, args []string) error {
	t := env.Config.(*settings)
	t.Bucket = ""
	s, err := storeFromEnv(env)
	if err != nil {
		return err
	}

	ctx := getContext(env)
	defer blob.CloseStore(ctx, s)

	var msg []byte
	switch t := s.(type) {
	case chirpstore.CAS:
		msg, err = t.Status(ctx)
	default:
		return errors.New("server does not support the status command")
	}
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	json.Indent(&buf, msg, "", "  ")
	fmt.Println(buf.String())
	return nil
}

func putCmd(env *command.Env, args []string) (err error) {
	if len(args) == 0 || len(args) > 2 {
		return errors.New("usage is: put <key> [<path>]")
	}
	key, err := parseKey(args[0])
	if err != nil {
		return err
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return nil
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, bs)

	data, err := readData(ctx, "put", args[1:])
	if err != nil {
		return err
	}

	return bs.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    data,
		Replace: env.Config.(*settings).Replace,
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

func storeFromEnv(env *command.Env) (blob.CAS, error) {
	t := env.Config.(*settings)
	addr, ok := t.FFS.FindAddress()
	if !ok {
		return nil, fmt.Errorf("no -store address was found (%q)", addr)
	}

	conn, err := net.Dial(chirp.SplitAddress(addr))
	if err != nil {
		return nil, fmt.Errorf("dialing: %w", err)
	}

	peer := chirp.NewPeer().Start(channel.IO(conn, conn))
	if t.Debug {
		peer.LogPackets(func(pkt chirp.PacketInfo) { log.Print(pkt) })
	}
	bs := chirpstore.NewCAS(peer, nil)
	if t.Bucket == "" {
		return bs, nil
	}
	return prefixed.NewCAS(bs).Derive(t.Bucket), nil
}

func isAllHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func parseKey(s string) (string, error) {
	if strings.HasPrefix(s, "@") {
		return s[1:], nil
	}
	var key []byte
	var err error
	if isAllHex(s) {
		key, err = hex.DecodeString(s)
	} else if strings.HasSuffix(s, "=") {
		key, err = base64.StdEncoding.DecodeString(s)
	} else {
		key, err = base64.RawStdEncoding.DecodeString(s) // tolerate missing padding
	}
	if err != nil {
		return "", fmt.Errorf("invalid key %q: %w", s, err)
	}
	return string(key), nil
}
