// Package zstdc implements the encoded.Codec interface using zstd compression.
package zstdc

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// A Codec implements the encoded.Codec interface to provide zstd compression
// of blob data.
type Codec struct {
	e *zstd.Encoder
	d *zstd.Decoder
}

// New returns a new zstd codec with default compression settings.
func New() Codec {
	e, _ := zstd.NewWriter(nil) // no error is possible without options
	d, _ := zstd.NewReader(nil) // no error is possible without options
	return Codec{e: e, d: d}
}

// Encode compresses src via zstd and writes it to w.
func (c Codec) Encode(w io.Writer, src []byte) error {
	buf := c.e.EncodeAll(src, make([]byte, 0, c.e.MaxEncodedSize(len(src))))
	_, err := w.Write(buf)
	return err
}

// Decode decompresses src via zstd and writes it to w.
func (c Codec) Decode(w io.Writer, src []byte) error {
	buf, err := c.d.DecodeAll(src, nil)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}
