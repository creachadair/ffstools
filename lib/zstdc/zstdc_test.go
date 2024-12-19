package zstdc_test

import (
	"testing"

	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/encoded"
	"github.com/creachadair/ffstools/lib/zstdc"
)

func TestStore(t *testing.T) {
	m := encoded.New(memstore.New(nil), zstdc.New())
	storetest.Run(t, storetest.NopCloser(m))
}
