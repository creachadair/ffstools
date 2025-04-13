// Package scanlib implements a basic reachability scanner for FFS
// data blobs, files, and root pointers.
package scanlib

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"sort"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/mds/slice"
)

// Type represents the type of a blob.
type Type byte

const (
	Data  Type = '-' // An unstructured data blob
	File  Type = 'F' // A file record (Node)
	Root  Type = 'R' // A root record (Root)
	Index Type = 'I' // An index record (Index)
)

// A Scanner scans all the blobs reachable from a collection of root and file
// pointers.
type Scanner struct {
	keys map[string]Type
	src  blob.CAS
}

// NewScanner creates a new empty Scanner that reads data from src.
func NewScanner(src blob.CAS) *Scanner {
	return &Scanner{keys: make(map[string]Type), src: src}
}

// RootOnly adds the specified root to the scan, including its index (if any),
// but excluding any blobs reachable from its file pointer.
//
// Use [Scanner.Root] to completely scan a root.
func (s *Scanner) RootOnly(rootKey string, rp *root.Root) {
	s.keys[rootKey] = Root
	if ik := rp.IndexKey; ik != "" {
		s.keys[rp.IndexKey] = Index
	}
}

// Root adds the specified root, its index (if any), and all files reachable
// from its file pointer to s.
func (s *Scanner) Root(ctx context.Context, rootKey string, rp *root.Root) error {
	s.RootOnly(rootKey, rp)
	fp, err := rp.File(ctx, s.src)
	if err != nil {
		return err
	}
	return s.File(ctx, fp)
}

// File adds all the files and data blobs reachable from fp to s.
func (s *Scanner) File(ctx context.Context, fp *file.File) error {
	return fp.Scan(ctx, func(si file.ScanItem) bool {
		key := si.Key()
		if _, ok := s.keys[key]; ok {
			return false // skip repeats of the same file
		}
		s.keys[key] = File

		// Record all the data blocks.
		for _, dkey := range si.Data().Keys() {
			s.Data(dkey)
		}
		return true
	})
}

// Data adds the specified data blob to s.
func (s *Scanner) Data(key string) { s.keys[key] = Data }

// Len reports the total number of keys in s, of all kinds.
func (s *Scanner) Len() int { return len(s.keys) }

// IsRoot reports whether key is recorded as a root pointer in s.
func (s *Scanner) IsRoot(key string) bool { return s.keys[key] == Root }

// Remove removes the specified key from s.
func (s *Scanner) Remove(key string) { delete(s.keys, key) }

// Type reports the type of the specified key. It panics if key is not in s.
func (s *Scanner) Type(key string) Type {
	if k, ok := s.keys[key]; ok {
		return k
	}
	panic(fmt.Sprintf("no such key %x", key))
}

// All returns an iterator over all the key/type pairs in s.
func (s *Scanner) All() iter.Seq2[string, Type] { return maps.All(s.keys) }

// Chunks returns consecutive chunks of the keys in s each having at most n
// elements and together spanning the complete set of keys recorded.  The keys
// are returned in lexicographic order.
func (s *Scanner) Chunks(n int) iter.Seq[[]string] {
	all := slice.MapKeys(s.keys)
	sort.Strings(all)
	return func(yield func([]string) bool) {
		i := 0
		for i < len(all) {
			end := min(i+n, len(all))
			if !yield(all[i:end]) {
				return
			}
			i = end
		}
	}
}

// Stats records aggregate statistics about a scan.
type Stats struct {
	NumRoots int // number of distinct roots
	NumFiles int // number of distinct files
	NumBlobs int // number of distinct data blobs
}

// Stats returns aggregate statistics about everything scanned in s.
func (s *Scanner) Stats() Stats {
	var out Stats
	for _, kind := range s.keys {
		switch kind {
		case Data, Index:
			out.NumBlobs++
		case Root:
			out.NumRoots++
		case File:
			out.NumFiles++
		default:
			panic(fmt.Sprintf("unexpected key type %q", kind))
		}
	}
	return out
}
