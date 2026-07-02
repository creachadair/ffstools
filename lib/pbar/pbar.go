// Package pbar implements a simple percentage-based terminal progress
// indicator.
package pbar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// A Bar is a progress indicator. A nil *Bar is ready for use, but
// ignores its input and does nothing.
// The update methods of a Bar (Add, Set, SetMeta) are safe for concurrent use
// by multiple goroutines.
type Bar struct {
	w      io.Writer
	cancel context.CancelFunc
	done   chan struct{}
	pulse  *time.Ticker

	μ        sync.Mutex
	cur, max int64
	meta     int64
}

// New contructs a new Bar with the given maximum value that writes a status
// line periodically to w. Call Start to begin updating the progress line, and
// Stop to stop it.
func New(w io.Writer, max int64) *Bar { return &Bar{w: w, max: max} }

// Start begins rendering the status line for b. It returns b to permit
// chaining. If b == nil, it does nothing without error.
func (b *Bar) Start() *Bar {
	if b == nil || b.done != nil {
		return b // cannot start, or already running
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.done = make(chan struct{})
	b.cancel = cancel
	b.pulse = time.NewTicker(time.Second)
	go func() {
		defer b.pulse.Stop()
		defer close(b.done)
		for {
			select {
			case <-b.pulse.C:
				b.repaint()
			case <-ctx.Done():
				b.repaint()
				fmt.Fprintln(b.w, " *")
				return
			}
		}
	}()
	return b
}

// Stop stops rendering the status line for b.
// If b == nil, it does nothing without error.
func (b *Bar) Stop() {
	if b == nil {
		return
	}
	b.cancel()
	<-b.done
}

// Set sets the current value of the bar to v. If v exceeds the current
// maximum, the bar length is extended.
func (b *Bar) Set(v int64) {
	if b == nil {
		return
	}
	b.μ.Lock()
	defer b.μ.Unlock()
	if v != b.cur {
		b.cur = v
		if v > b.max {
			b.max = v
		}
	}
}

// SetMeta sets the current value of the meta-counter.
func (b *Bar) SetMeta(v int64) {
	if b == nil {
		return
	}
	b.μ.Lock()
	defer b.μ.Unlock()
	b.meta = v
}

// Add adds v to the current value of the bar. The offset may be negative.  If
// the resulting value is less than zero it is pinned to zero. If the resulting
// value exceeds the current maximum the bar length is extended.
func (b *Bar) Add(v int64) {
	if b == nil {
		return
	}
	b.μ.Lock()
	defer b.μ.Unlock()
	b.cur += v
	if b.cur < 0 {
		b.cur = 0
	} else if b.cur > b.max {
		b.max = b.cur
	}
}

func (b *Bar) repaint() {
	b.μ.Lock()
	cur, max, meta := b.cur, b.max, b.meta
	b.μ.Unlock()

	var buf bytes.Buffer
	buf.WriteString("\r[")
	fr := float64(cur) / float64(max)
	nc, lip := int(fr*50), int(fr*100)%2 == 1
	buf.WriteString(strings.Repeat("=", nc))
	if lip {
		buf.WriteByte('-')
		nc++
	}
	buf.WriteString(strings.Repeat(" ", 50-nc))
	fmt.Fprintf(&buf, "] %.1f%%", 100*fr)
	if meta != 0 {
		fmt.Fprintf(&buf, " %d", meta)
	}
	b.w.Write(buf.Bytes())
}
