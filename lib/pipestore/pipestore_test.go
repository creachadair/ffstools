package pipestore_test

import (
	"testing"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffstools/lib/pipestore"
)

func TestConnectAndStore(t *testing.T) {
	ch, r, w, err := pipestore.Connect()
	if err != nil {
		t.Fatalf("Connect failed; %v", err)
	}

	sp := chirp.NewPeer().Start(ch)
	t.Cleanup(func() { sp.Stop() })

	st := chirpstore.NewService(memstore.New(nil), nil)
	st.Register(sp)

	cli := pipestore.New(r, w)
	storetest.Run(t, cli)
}
