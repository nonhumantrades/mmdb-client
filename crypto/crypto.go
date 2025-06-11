package crypto

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/nonhumantrades/mmdb-proto/proto"
	"github.com/pierrec/lz4/v4"
)

func Hash(data []byte) uint64 {
	h := xxhash.New()
	h.Write(data)
	return h.Sum64()
}

type Compression struct {
	noCompression Compressor
	snappy        Compressor
	lz4           Compressor
	zstd          Compressor
}

func NewCompressor() (*Compression, error) {
	zstd, err := NewZstdCompressor(int(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	return &Compression{
		noCompression: &NoCompressionCompressor{},
		snappy:        &SnappyCompressor{},
		lz4:           NewLZ4Compressor(),
		zstd:          zstd,
	}, nil
}

func (c *Compression) Get(method int32) Compressor {
	switch method {
	case int32(proto.CompressionMethod_CompressionNone):
		return c.noCompression
	case int32(proto.CompressionMethod_CompressionSnappy):
		return c.snappy
	case int32(proto.CompressionMethod_CompressionLZ4):
		return c.lz4
	case int32(proto.CompressionMethod_CompressionZstd):
		return c.zstd
	default:
		return c.noCompression
	}
}

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

type NoCompressionCompressor struct{}

func (c *NoCompressionCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (c *NoCompressionCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

type SnappyCompressor struct{}

func (c *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

type LZ4Compressor struct {
	writerPool *sync.Pool
	readerPool *sync.Pool
}

type lz4Writer struct {
	buf    *bytes.Buffer
	writer *lz4.Writer
}

func NewLZ4Compressor() *LZ4Compressor {
	p := &LZ4Compressor{}
	p.writerPool = &sync.Pool{
		New: func() any {
			buf := &bytes.Buffer{}
			w := lz4.NewWriter(buf)
			return &lz4Writer{buf: buf, writer: w}
		},
	}
	p.readerPool = &sync.Pool{
		New: func() any {
			return lz4.NewReader(nil)
		},
	}
	return p
}

func (c *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	v := c.writerPool.Get().(*lz4Writer)
	buf := v.buf
	w := v.writer
	buf.Reset()
	w.Reset(buf)
	if _, err := w.Write(data); err != nil {
		c.writerPool.Put(v)
		return nil, err
	}
	if err := w.Close(); err != nil {
		c.writerPool.Put(v)
		return nil, err
	}
	out := slices.Clone(buf.Bytes())
	c.writerPool.Put(v)
	return out, nil
}

func (c *LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	r := c.readerPool.Get().(*lz4.Reader)
	r.Reset(bytes.NewReader(data))
	out, err := io.ReadAll(r)
	c.readerPool.Put(r)
	return out, err
}

type ZstdCompressor struct {
	level       int
	encoderPool *sync.Pool
	decoderPool *sync.Pool
}

type zstdErr struct{ err error }

func NewZstdCompressor(level int) (*ZstdCompressor, error) {
	zc := &ZstdCompressor{level: level}

	zc.encoderPool = &sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
			if err != nil {
				return &zstdErr{err: err}
			}
			return enc
		},
	}
	zc.decoderPool = &sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}

	if v := zc.encoderPool.Get(); v != nil {
		if e, ok := v.(*zstdErr); ok {
			return nil, e.err
		}
		zc.encoderPool.Put(v)
	}

	return zc, nil
}

func (c *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	v := c.encoderPool.Get()
	switch t := v.(type) {
	case *zstdErr:
		return nil, t.err
	case *zstd.Encoder:
		out := t.EncodeAll(data, nil)
		c.encoderPool.Put(t)
		return out, nil
	default:
		return nil, fmt.Errorf("zstd encoder pool returned unexpected type %T", v)
	}
}

func (c *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	dec := c.decoderPool.Get().(*zstd.Decoder)
	out, err := dec.DecodeAll(data, nil)
	c.decoderPool.Put(dec)
	return out, err
}
