// Program ffsgocache implements the GOCACHEPROG protocol to implement
// a cache for the Go toolchain.
//
// Example Usage
//
//	GOCACHEPROG="ffsgocache --cache-dir /data/gocache --store db.sock" go test ./...
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/lib/storeclient"
	"github.com/creachadair/flax"
	"github.com/creachadair/gocache"
	"github.com/creachadair/gocache/cachedir"
	"github.com/creachadair/mds/value"
	"github.com/creachadair/taskgroup"
)

var flags = struct {
	CacheDir     string `flag:"cache-dir,default=$FFS_GOCACHE,Cache directory (required)"`
	Store        string `flag:"store,default=$FFS_STORE,Address of storage service (required)"`
	RootName     string `flag:"root,default=$FFS_ROOTNAME,Name of cache root (required)"`
	Tasks        int    `flag:"nw,default=*,PRIVATE:Number of concurrent upload tasks"`
	NoUpdate     bool   `flag:"no-update,Do not update the cache root at exit"`
	PrintMetrics bool   `flag:"m,Print summary metrics to stderr at exit"`
	Verbose      bool   `flag:"v,Enable verbose logging"`
	DebugLog     bool   `flag:"debug,Enable detailed per-request debug logging (warning: noisy)"`
}{
	Tasks: runtime.NumCPU(),
}

func main() {
	root := &command.C{
		Name:     command.ProgramName(),
		Usage:    "<flags>\nhelp",
		Help:     `Serve the GOCACHEPROG protocol on stdio to an FFS store.`,
		SetFlags: command.Flags(flax.MustBind, &flags),
		Run:      command.Adapt(runCache),
		Commands: []*command.C{
			command.HelpCommand(nil),
			command.VersionCommand(),
		},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	command.RunOrFail(root.NewEnv(nil).SetContext(ctx), os.Args[1:])
}

func runCache(env *command.Env) error {
	switch {
	case flags.CacheDir == "":
		return env.Usagef("missing required --cache-dir")
	case !filepath.IsAbs(flags.CacheDir):
		return env.Usagef("the --cache-dir must be an absolute path")
	case flags.Store == "":
		return env.Usagef("missing required --store address")
	case flags.RootName == "":
		return env.Usagef("missing required --root name")
	}

	// Create a local cache directory to send files to the toolchain.
	// Replies to Get requests will point into this directory.
	cd, err := cachedir.New(flags.CacheDir)
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}

	// Connect to the storage service for the backing store.
	st, err := storeclient.ParseAddress(flags.Store).Connect(env.Context(), nil)
	if err != nil {
		return fmt.Errorf("dial store: %w", err)
	}
	ft, err := filetree.NewStore(env.Context(), st)
	if err != nil {
		return err
	}

	// Each invocation of the tool specifies which cache root it will use.
	// If that root already exists, the cache will be its existing contents;
	// otherwise we create a new empty one.
	rp, err := root.Open(env.Context(), ft.Roots(), flags.RootName)
	if blob.IsKeyNotFound(err) {
		rp = root.New(ft.Roots(), nil) // OK, create a new one
	} else if err != nil {
		return fmt.Errorf("open root: %w", err)
	}
	fp, err := rp.File(env.Context(), ft.Files())
	if errors.Is(err, root.ErrNoData) {
		fp = file.New(ft.Files(), &file.NewOptions{
			Stat:        &file.Stat{Mode: fs.ModeDir | 0755}, // for cosmetics
			PersistStat: true,
		}) // OK, create a new one
	} else if err != nil {
		return fmt.Errorf("open root file: %w", err)
	}

	// Maintain a limited pool of goroutines for writing data back to the store.
	g, start := taskgroup.New(func(err error) {
		log.Printf("WARNING: writeback error: %v", err)
	}).Limit(flags.Tasks)
	defer g.Wait()

	fc := ffsCache{dir: cd, root: fp, start: start}
	gc := &gocache.Server{
		Get:         fc.Get,
		Put:         fc.Put,
		MaxRequests: flags.Tasks,
		Logf:        value.Cond(flags.Verbose || flags.DebugLog, log.Printf, nil),
		LogRequests: flags.DebugLog,
	}
	if err := gc.Run(env.Context(), os.Stdin, os.Stdout); err != nil {
		return fmt.Errorf("cache server exited with error: %w", err)
	}
	g.Wait() // wait for writes to settle

	// Unless we were instructed not to, update the specified root with the
	// final state of the cache at exit.
	if !flags.NoUpdate {
		fk, err := fp.Flush(env.Context())
		if err != nil {
			return fmt.Errorf("flush cache: %w", err)
		}
		if rp.FileKey != fk {
			rp.FileKey = fk
			rp.IndexKey = "" // invalidate
			if err := rp.Save(env.Context(), flags.RootName); err != nil {
				return fmt.Errorf("update cache root: %w", err)
			}
		}
	}
	if flags.Verbose || flags.PrintMetrics {
		fmt.Fprintln(env, gc.Metrics())
	}
	return nil
}

type ffsCache struct {
	dir   *cachedir.Dir
	root  *file.File
	start taskgroup.StartFunc
}

const outputIDAttr = "output-id"

func (c ffsCache) Get(ctx context.Context, actionID string) (outputID, diskPath string, _ error) {
	if objID, diskPath, err := c.dir.Get(ctx, actionID); err == nil && objID != "" && diskPath != "" {
		return objID, diskPath, nil // cache hit, OK
	}
	fp, err := c.root.Open(ctx, actionID)
	if errors.Is(err, file.ErrChildNotFound) {
		return "", "", nil // cache miss
	} else if err != nil {
		return "", "", err
	}
	outputID = fp.XAttr().Get(outputIDAttr)
	diskPath, err = c.dir.Put(ctx, gocache.Object{
		ActionID: actionID,
		OutputID: outputID,
		Size:     fp.Data().Size(),
		Body:     fp.Cursor(ctx),
		ModTime:  fp.Stat().ModTime,
	})
	return outputID, diskPath, err
}

func (c ffsCache) Put(ctx context.Context, req gocache.Object) (diskPath string, _ error) {
	// We need to read the data twice, once to update the cache directory, and
	// again to write back to the underlying store.
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	} else if int64(len(data)) != req.Size {
		return "", fmt.Errorf("size mismatch: got %d bytes, want %d", len(data), req.Size)
	}

	req.Body = bytes.NewReader(data)
	diskPath, err = c.dir.Put(ctx, req)
	if err != nil {
		return "", err
	}

	// Special case: Do not bother storing zero-length outputs in the backing store.
	// Such outputs are fairly common, and it's cheaper to let the toolchain recompute
	// them than to store and fetch them. We'll still store them in the local cache.
	if len(data) == 0 {
		return diskPath, nil
	}

	c.start.Go(func() error {
		mt := req.ModTime
		if mt.IsZero() {
			mt = time.Now()
		}
		fp := c.root.New(&file.NewOptions{
			// Save the modification time to report back on Get, and since we are
			// doing that anyway, persist a mode for cosmetics.
			Stat:        &file.Stat{Mode: 0644, ModTime: mt.UTC()},
			PersistStat: true,
		})
		fp.XAttr().Set(outputIDAttr, req.OutputID)
		if err := fp.SetData(ctx, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("action %q: write data: %w", req.ActionID, err)
		}
		c.root.Child().Set(req.ActionID, fp)
		return nil
	})
	return diskPath, nil
}
