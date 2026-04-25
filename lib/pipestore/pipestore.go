// Package pipestore implements an interface to a [blob.Store] that communicates
// via Chirp v0 (using [chirpstore]) over a pair of pipes.
package pipestore

import (
	"os"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
)

// Connect establishes a connected pair of pipes for a server and a client,
// returning a [chirp.Channel] for the server and the read and write pipes for
// the client. Each call to Connect returns a distinct pair of pipes.
func Connect() (_ chirp.Channel, r, w *os.File, _ error) {
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

// New constructs a new [blob.StoreCloser] that communicates over the specified
// pipes. The concrete type of the result is [chirpstore.Store].
func New(rf, wf *os.File) blob.StoreCloser {
	peer := chirp.NewPeer().Start(channel.IO(rf, wf))
	return chirpstore.NewStore(peer, nil)
}
