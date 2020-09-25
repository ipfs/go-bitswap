package network

import (
	"compress/gzip"
	"sync"

	"github.com/libp2p/go-libp2p-core/network"
	"go.uber.org/multierr"
)

type compressedStream struct {
	network.Stream
	wlock sync.Mutex
	w     *gzip.Writer

	r *gzip.Reader
}

func compressStream(s network.Stream) network.Stream {
	return &compressedStream{Stream: s, w: gzip.NewWriter(s)}
}

func (c *compressedStream) Write(b []byte) (int, error) {
	c.wlock.Lock()
	defer c.wlock.Unlock()
	n, err := c.w.Write(b)
	// TODO: ideally, we'd flush inside bitswap itself.
	return n, multierr.Combine(err, c.w.Flush())
}

func (c *compressedStream) Read(b []byte) (int, error) {
	if c.r == nil {
		// This _needs_ to be lazy as it reads a header.
		var err error
		c.r, err = gzip.NewReader(c.Stream)
		if err != nil {
			return 0, err
		}
	}
	n, err := c.r.Read(b)
	if err != nil {
		c.r.Close()
	}
	return n, err
}

func (c *compressedStream) Close() error {
	c.wlock.Lock()
	defer c.wlock.Unlock()
	return multierr.Combine(c.w.Close(), c.Stream.Close())
}
