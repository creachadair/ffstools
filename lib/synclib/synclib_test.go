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

package synclib_test

import (
	"fmt"
	"io"
	"io/fs"
	"testing"

	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/filetree/filetreetest"
	"github.com/creachadair/ffstools/lib/synclib"
)

func TestConfig(t *testing.T) {
	mem := memstore.New(nil)
	s, err := filetree.NewStore(t.Context(), mem)
	if err != nil {
		t.Fatalf("Create store: %v", err)
	}

	// Create a small tree of files with a root.
	filetreetest.SetRoot(t, s, "testing", nil)
	filetreetest.SetFile(t, s, filetreetest.FileInfo{
		Path: "testing/dir",
		Mode: fs.ModeDir | 0755,
	})
	filetreetest.SetFile(t, s, filetreetest.FileInfo{
		Path:    "testing/dir/file1",
		Mode:    0644,
		Content: "hello, world",
	})
	filetreetest.SetFile(t, s, filetreetest.FileInfo{
		Path:    "testing/file2",
		Mode:    0600,
		Content: "character is what you are in the dark",
		XAttr: map[string]string{
			"apple": "pear",
			"plum":  "cherry",
		},
	})

	// Sync this tree to a different store.
	mem2 := memstore.New(nil)
	s2, err := filetree.NewStore(t.Context(), mem2)
	if err != nil {
		t.Fatalf("Create store 2: %v", err)
	}
	var nlogs int
	sc := synclib.Config{
		Source: s,
		Target: s2,
		Progress: func(level int, msg string, args ...any) {
			nlogs++
			t.Logf("log #%d [%d]: %s", nlogs, level, fmt.Sprintf(msg, args...))
		},
		ProgressLevel: synclib.ProgressDebug,
	}

	// Some basic health checks on the results of sync.
	st, err := sc.Sync(t.Context(), []string{"testing"})
	if err != nil {
		t.Errorf("Sync failed: %v", err)
	}
	if got, want := nlogs, 3; got != want {
		t.Errorf("Got %d logs, want %d", got, want)
	}
	if got, want := st.Copied, int64(7); got != want {
		// 7 ← +1 root, +1 base, +1 dir, +2 file1, +2 file2
		// each file has +1 for the file itself, +1 for its content
		t.Errorf("Copied %d objects, want %d", got, want)
	}

	// Verify we got compatible data on the target side.
	f1 := filetreetest.GetFile(t, s2, "testing/dir/file1")
	if data, err := io.ReadAll(f1.File.Cursor(t.Context())); err != nil {
		t.Errorf("Read file1: %v", err)
	} else if got, want := string(data), "hello, world"; got != want {
		t.Errorf("Data for file1: got %q, want %q", got, want)
	}
	f2 := filetreetest.GetFile(t, s2, "testing/file2")
	if got, want := f2.File.XAttr().Get("apple"), "pear"; got != want {
		t.Errorf("XAttr for file2: got %q, want %q", got, want)
	}
}
