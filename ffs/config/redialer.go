package config

import (
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"
)

func Dial(ntype, addr string) (net.Conn, error) {
	c, err := net.Dial(ntype, addr)
	if err != nil {
		return nil, err
	} else if ntype == "unix" {
		return c, nil
	}
	return &redialConn{
		Conn:        c,
		net:         ntype,
		addr:        addr,
		maxAttempts: 3,
		grace:       30 * time.Second,
		lastSuccess: time.Now(),
	}, nil
}

type redialConn struct {
	mu sync.Mutex
	net.Conn

	net, addr   string
	maxAttempts int           // total attempts allowed per successful connection
	numAttempts int           // attempts since latest grace interval
	grace       time.Duration // interval beyond which attempts are reset
	lastSuccess time.Time     // last successful dial
}

func (r *redialConn) tryRedialLocked() error {
	if time.Since(r.lastSuccess) > r.grace {
		r.numAttempts = 0
	} else if r.numAttempts >= r.maxAttempts {
		return fmt.Errorf("maximum %d redials exhausted", r.maxAttempts)
	}
	r.numAttempts++
	c, err := net.Dial(r.net, r.addr)
	if err != nil {
		return err
	}
	r.Conn = c
	r.lastSuccess = time.Now()
	return nil
}

func (r *redialConn) Read(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		nr, err := r.Conn.Read(data)
		if err != nil {
			if isRedialableError(err) && r.tryRedialLocked() == nil {
				continue // retry the failed operation
			}
		}
		return nr, err
	}
}

func (r *redialConn) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		nw, err := r.Conn.Write(data)
		if err != nil {
			if isRedialableError(err) && r.tryRedialLocked() == nil {
				continue // retry the failed operation
			}
		}
		return nw, err
	}
}

func isRedialableError(err error) bool {
	if e, ok := err.(syscall.Errno); ok {
		return e == syscall.ECONNRESET || e == syscall.ECONNABORTED
	}
	return false
}
