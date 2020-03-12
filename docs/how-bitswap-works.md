How Bitswap Works
=================

When a client requests blocks, Bitswap sends the CID of those blocks to its peers as "wants". When Bitswap receives a "want" from a peer, it responds with the corresponding block.

### Requesting Blocks

#### Sessions

Bitswap Sessions allow the client to make related requests to the same group of peers. For example typically requests to fetch all the blocks in a file would be made with a single session.

#### Discovery

To discover which peers have a block, Bitswap broadcasts a `want-have` message to all peers it is connected to asking if they have the block.

Any peers that have the block respond with a `HAVE` message. They are added to the Session.

If no connected peers have the block, Bitswap queries the DHT to find peers that have the block.

### Requesting Blocks

When the client requests a block, Bitswap sends a `want-have` message with the block CID to all peers in the Session to ask who has the block.

Bitswap simultaneously sends a `want-block` message to one of the peers in the Session to request the block. If the peer does not have the block, it responds with a `DONT_HAVE` message. In that case Bitswap selects another peer and sends the `want-block` to that peer.

If no peers have the block, Bitswap broadcasts a `want-have` to all connected peers, and queries the DHT to find peers that have the block.

#### Peer Selection

Bitswap uses a probabilistic algorithm to select which peer to send `want-block` to, favouring peers that
- sent `HAVE` for the block
- were discovered as providers of the block in the DHT
- were first to send blocks to previous session requests

The selection algorithm includes some randomness so as to allow peers that are discovered later, but are more responsive, to rise in the ranking.

#### Periodic Search Widening

Periodically the Bitswap Session selects a random CID from the list of "pending wants" (wants that have been sent but for which no block has been received). Bitswap broadcasts a `want-have` to all connected peers and queries the DHT for the CID.

### Serving Blocks

#### Processing Requests

When Bitswap receives a `want-have` it checks if the block is in the local blockstore.

If the block is in the local blockstore Bitswap responds with `HAVE`. If the block is small Bitswap sends the block itself instead of `HAVE`.

If the block is not in the local blockstore, Bitswap checks the `send-dont-have` flag on the request. If `send-dont-have` is true, Bitswap sends `DONT_HAVE`. Otherwise it does not respond.

#### Processing Incoming Blocks

When Bitswap receives a block, it checks to see if any peers sent `want-have` or `want-block` for the block. If so it sends `HAVE` or the block itself to those peers.

#### Priority

Bitswap keeps requests from each peer in separate queues, ordered by the priority specified in the request message.

To select which peer to send the next response to, Bitswap chooses the peer with the least amount of data in its send queue. That way it will tend to "keep peers busy" by always keeping some data in each peer's send queue.

