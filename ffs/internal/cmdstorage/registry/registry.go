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

// Package registry carries the registry of available storage implementations.
// This is a separate package to ensure it is fully initialized before the
// parent package looks at the store.
package registry

import (
	"archive/zip"
	"context"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/ffs/storage/zipstore"
	"github.com/creachadair/ffstools/lib/store"
)

// Stores enumerates the storage implementations are built in by default.
// To include other stores, build with -tags set to their names.  The known
// implementations are in the store_*.go files.
var Stores = store.Registry{
	"file": func(ctx context.Context, addr string) (blob.Store, error) {
		if strings.HasSuffix(addr, ".zip") {
			zf, err := zip.OpenReader(addr)
			if err != nil {
				return nil, err
			}
			return zipstore.New(zf, nil), nil
		}
		return filestore.Opener(ctx, addr)
	},
	"memory": memstore.Opener,
}
