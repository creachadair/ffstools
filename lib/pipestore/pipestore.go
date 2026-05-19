// Package pipestore implements a [chirp.Channel] using a pair of pipes.
package pipestore

import (
	"context"
	"net"
	"os"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
)

// Connect establishes a connected pair of pipes for a server and a client,
// returning a [chirp.Channel] for the server one for the client.
// Each call to Connect returns a distinct pair of pipes.
func Connect() (_ chirp.Channel, cr, cw *os.File, _ error) {
	sr, cw, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, err
	}
	cr, sw, err := os.Pipe()
	if err != nil {
		sr.Close()
		cw.Close()
		return nil, nil, nil, err
	}
	return channel.IO(sr, sw), cr, cw, nil
}

type recv struct {
	pkt chirp.Packet
	err error
}

// NewChannel constructs a new [Channel] wrapping the specified files.
func NewChannel(r, w *os.File) Channel {
	ch := channel.IO(r, w)
	ctx, cancel := context.WithCancel(context.Background())
	req := make(chan struct{})
	rsp := make(chan recv)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-req:
				pkt, err := ch.Recv()
				select {
				case rsp <- recv{pkt: pkt, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return Channel{
		ctx:    ctx,
		cancel: cancel,
		req:    req,
		rsp:    rsp,
		ch:     ch,
	}
}

// A Channel implements the [chirp.Channel] interface around a pipe whose read
// and write endpoints are provided.
type Channel struct {
	ctx    context.Context
	cancel context.CancelFunc
	req    chan<- struct{}
	rsp    <-chan recv
	ch     channel.IOChannel
}

// Send implements part of the [chirp.Channel] interface.
func (c Channel) Send(pkt chirp.Packet) error {
	if c.ctx.Err() != nil {
		return net.ErrClosed
	}
	return c.ch.Send(pkt)
}

// Recv implements part of the [chirp.Channel] interface.
func (c Channel) Recv() (chirp.Packet, error) {
	// When sharing a pipe with multiple descendants, e.g., when the child
	// process is a shell that will execute other subprocesses that can access
	// the pipe, the shell may not (in general) close the write end of its dup
	// of the pipe. This means a child may not get EOF from the reader after
	// closing its write half, since there is another dup still active.
	//
	// Calling [os.File.Close] does not suffice, as the polling shims can defer
	// closing the actual file descriptor. Moreover, even if we close the
	// descriptor explicitly, that may not unblock a poll. To avoid blocking
	// indefinitely in that case, perform the read in a goroutine and give up if
	// the channel closes before we get an answer.
	select {
	case <-c.ctx.Done():
		// closed
	case c.req <- struct{}{}:
		select {
		case <-c.ctx.Done():
			break
		case r := <-c.rsp:
			return r.pkt, r.err
		}
	}
	return chirp.Packet{}, net.ErrClosed
}

// Close implements part of the [chirp.Channel] interface.
func (c Channel) Close() error {
	c.cancel()
	<-c.ctx.Done()
	return c.ch.Close()
}
