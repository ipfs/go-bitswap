module github.com/ipfs/go-bitswap

require (
	github.com/cskr/pubsub v1.0.2
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-cid v0.0.3
	github.com/ipfs/go-datastore v0.1.1
	github.com/ipfs/go-detect-race v0.0.1
	github.com/ipfs/go-ipfs-blockstore v0.1.0
	github.com/ipfs/go-ipfs-blocksutil v0.0.1
	github.com/ipfs/go-ipfs-delay v0.0.1
	github.com/ipfs/go-ipfs-exchange-interface v0.0.1
	github.com/ipfs/go-ipfs-pq v0.0.1
	github.com/ipfs/go-ipfs-routing v0.1.0
	github.com/ipfs/go-ipfs-util v0.0.1
	github.com/ipfs/go-log v0.0.1
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-peertaskqueue v0.1.2-0.20191028140841-a62f9b47895a
	github.com/jbenet/goprocess v0.1.3
	github.com/libp2p/go-buffer-pool v0.0.2
	github.com/libp2p/go-libp2p v0.4.0
	github.com/libp2p/go-libp2p-core v0.2.3
	github.com/libp2p/go-libp2p-loggables v0.1.0
	github.com/libp2p/go-libp2p-netutil v0.1.0
	github.com/libp2p/go-libp2p-protocol v0.1.0
	github.com/libp2p/go-libp2p-testing v0.1.0
	github.com/libp2p/go-msgio v0.0.4
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/multiformats/go-multiaddr v0.1.1
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/prometheus/common v0.7.0
)

// replace github.com/ipfs/go-peertaskqueue => ../go-peertaskqueue

go 1.12
