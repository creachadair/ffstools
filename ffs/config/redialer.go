package config

import (
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"
)

// Dial calls net.Dial with the specified arguments.  If they succeed and ntype
// is not "unix", the resulting connection is wrapped to redial the address if
// the connection fails during a read or write (ECONNRESET or ECONNABORTED).
//
// If redial succeeds, the failed Read or Write operation is transparently
// retried; otherwise the original error is propagated to the caller.
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
	// Must hold mu exclusively to read or write the connection.
	mu sync.Mutex
	net.Conn

	net, addr   string
	maxAttempts int           // total attempts allowed per successful connection
	numAttempts int           // attempts since latest grace interval
	grace       time.Duration // interval beyond which attempts are reset
	lastSuccess time.Time     // last successful dial
}

func (r *redialConn) getConn() net.Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Conn
}

func (r *redialConn) tryRedial(old net.Conn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the connection was successfully replaced since we read the original
	// value, we can short-circuit here.
	if r.Conn != old {
		return nil
	}

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
	for {
		conn := r.getConn()
		nr, err := conn.Read(data)
		if err != nil {
			if isRedialableError(err) && r.tryRedial(conn) == nil {
				continue // retry the failed operation
			}
		}
		return nr, err
	}
}

func (r *redialConn) Write(data []byte) (int, error) {
	for {
		conn := r.getConn()
		nw, err := conn.Write(data)
		if err != nil {
			if isRedialableError(err) && r.tryRedial(conn) == nil {
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
