// Package pipestore implements an interface to a [blob.Store] that communicates
// via Chirp v0 (using [chirpstore]) over a pair of pipes.
package pipestore

import (
	"os"
	"syscall"

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

// NewChannel constructs a new [Channel] wrapping the specified files.
func NewChannel(r, w *os.File) Channel {
	return Channel{
		rf:        r,
		IOChannel: channel.IO(r, w),
	}
}

// A Channel implements the [chirp.Channel] interface around a pipe whose read
// and write endpoints are provided.
type Channel struct {
	rf *os.File
	channel.IOChannel
}

// Close implements a method of [chirp.Channel].
func (c Channel) Close() error {
	werr := c.IOChannel.Close()

	// When sharing a pipe with multiple child processes, e.g., when the child
	// process is a shell that will execute other subprocesses that will access
	// the pipe, the shell will not (in general) close the write end of its dup
	// of the pipe. This means a child will not get EOF from the reader after
	// closing its write half, since there is another dup still active.
	//
	// Calling [os.File.Close] will not suffice in that case, as the polling
	// shims can defer closing the actual file descriptor. So we'll apply blunt
	// force and do it explicitly. This interacts poorly if the descriptors are
	// NOT dup'd (e.g., in the parent process), but we don't need this type in
	// that case anyway.
	if rc, err := c.rf.SyscallConn(); err == nil {
		rc.Control(func(fd uintptr) { syscall.Close(int(fd)) })
	}
	return werr
}
