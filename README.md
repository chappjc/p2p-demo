# p2p-demo

1. peer exchange w/ simple Discoverer using a normal stream
2. manually gossip txids (nogossip.go), reannouncing only after retrieving content -- but have a gossipsub approach in gossip.go that is somewhat flawed
3. request/receive tx and block data on normal streams

We can use gossipsub for fast and broad gossip of small messages that do not require the announcing nodes to actually have underlying content.  For example, consensus messages or other network-wide status updates / heartbeat.

## libp2p

The main concepts in `libp2p` are: peers, streams, and protocols.

### Multiplexed Peer Connection

A connection to a peer is a persistent multiplexed connection.

- connection is persistent (connect/close)
- connection is multiplexed (concurrent communication protocols isolated with "streams")
- connection can use any transport or security (plain, tls, noise)
- peer ID is like address, derived from public key, authenticates peer

### Streams and Protocols

Within a multiplexed connection, there are any number of "streams" that isolate a communication "protocol".

- streams may be short lived or long lived, and are inexpensive to create
- a stream has a certain protocol: established message sequence that is application-defined and dictated by application code.
- a protocol is specified when opening the stream
- the actual protocol is transparent to libp2p, defined by application
- the expected sequence of messages, their encodings, and logic is what defines a protocol
- peer defines a "stream handler" to allow a peer to initiate a stream with a particular protocol
- peer creates a new stream instance for with a specific (1) peer, and (2) protocol
- either side of the new stream may initiate messaging, but it is normally the stream creator, not the stream handler
- the stream is multiplexed on the p2p connection with other steams, thus isolated and concurrent
- stream handlers and initiators define all timeouts, limits, etc. as the `Stream` is essentially an `io.ReadWriteCloser`, with other methods to get context like the remote peer

#### Example 1: `ProtocolIDTx`

This protocol is a simple request/response for getting the contents of a transaction.

```go
ProtocolIDTx       protocol.ID = "/kwil/tx/1.0.0"
```

The application currently defines the protocol as follows:

1. Stream creator sends a message prefixed with

  ```go
  getTxMsgPrefix  = "gettx:"
  ```

  If the stream creator wants a transaction ID `"1c577d897bb6cef3cffb8a6e323289eec5c85024dacd188d485fbf3bb003bb76"`, it `Write`s to the stream the message `"gettx:1c577d897bb6cef3cffb8a6e323289eec5c85024dacd188d485fbf3bb003bb76"`.

2. The stream handler waits for the message i.e. a blocking `Read`.

3. The stream handler parses the message (detect the prefix, and decode the txid), fetches the transaction.

4. If the transaction is not known or available, `Write` a `noData` message (just a `"0"` or something defined under this protocol).
   
5. If the transaction is known and available, `Write` the bytes of the transaction.

6. The stream creator waits for the response i.e. a blocking `Read`.

7. Both sides close the stream.

#### Example 2: `ProtocolIDTxAnn`

This protocol is of a dialog, although still relatively short-lived.

```go
ProtocolIDTxAnn    protocol.ID = "/kwil/txann/1.0.0"

annTxMsgPrefix  = "txann:"
```

Stream handler function `txAnnStreamHandler` handles the solicitation.

The function `advertiseTxToPeer` creates the new stream to advertise the transaction.

1. The announcer peer creates a stream to its peers.

2. It writes the message `"txann:1c577d8...` (full txid)

3. The stream handler peer reads and parses the message, and checks its inventory to see if it has or needs the transaction.

4. If the peer has the transaction already, it closes the stream. Fin.

5. If the peer needs the transaction content, it sends back a `"get"` message (on the same live stream).

6. The advertising peer recognizes the `"get"` response and sends the transaction contents.

7. The receiving peer receives and stores the transaction contents, and closes the stream.

The protocol ends there. At this point in the application, the stream handler would initiate async re-announce to other peers.

#### Other streams

There are also similar protocols for blocks, and a `ProtocolIDDiscover` for peer discovery with streams.

The peer discovery protocol is used by application methods that periodically request peer lists from its connected peers, which are added to its own peer store.  We will have to build logic around the peer store e.g. max nodes, types of nodes to connect to (only validators from leader, etc), etc.

The stream handler for `ProtocolIDDiscover` would just close the stream if the node had pex disabled. If enabled, the stream handler just sends it peer list on request. Here we can also build application logic, such as do not send certain peers depending on the identity of the requester.

## Kwil Protocols for Transactions, Blocks, and Consensus

### Overall Kwil Requirements

The following requirements are for the overall Kwil blockchain network design, and will dictate the specific P2P protocol requirements in the next section.

The network SHOULD ensure that the block producer (the leader) has all known transactions.

Blocks produced by the leader MUST propagate to the entire network. Validators SHOULD obtain the blocks quickly / with priority.

Consensus ACKs/NACKs from the validators SHOULD be returned to the leader quickly.

Leader COMMIT/ROLLBACK/ABRT should be announced and reach validators quickly.

Consensus messages MAY relay promiscuously (even through non-validators) to propagate quickly, but only validators process the messages.

All consensus and block messages MUST be signed and authenticated appropriately (blocks from leader, ACK/NACK from validator, COMMIT/ROLLBACK/ABRT from leader, etc.)

Transactions SHOULD be signed and/or validated prior to (re)announce, ingest into mempool, or included in block by leader.

To facilitate initial block download (IBD) to sync a node (outside of consensus process), peers MUST be able to:

- request blocks, sequentially or in batch
- handle block requests from all nodes, prioritizing validators (or recovering leader)
- sanity check retrieved block prior to executing it (height, prev block hash, other header elements)

To facilitate robust consensus in restart/reconnect scenarios, leader and validator:

- validators SHOULD re-send/broadcast ACK/NACK (containing their computed app hash for the block), conditionally if that block is latest
- validators SHOULD be able to solicit the leader's consensus status for a block (waiting for ACKs, or block already committed/aborted)
- leader SHOULD re-send COMMIT/ROLLBACK/ABRT periodically if insufficient responses received by a deadline
- validators SHOULD be able to send *unsolicited* "commit done" messages (normally a response in of the COMMIT/ROLLBACK/ABRT protocol)
- leader SHOULD respond to any out-of-sequence messages as appropriate for the validator to resolve
- (?) leader MAY propose a new block when insufficient responses (given timeout)

### Kwil P2P Requirements

- all nodes must announce new transactions created locally (e.g. received from RPC or authored by the node)
- nodes should retrieve unknown transactions that were announced to it
- nodes should re-announce the transactions that it has retrieved and which pass basic validity checks
- nodes should periodically re-announce unconfirmed transactions
- leader must broadcast new block IDs, and serve on request
- all nodes must be able to request specific blocks, by height or hash (maybe other identifiers)
- nodes may operate in "blocks only" mode, such as when syncing or when functioning as an archival node with no mempool

Other considerations arise from semantics of mempool and block confirmation. For instances, remove transactions from mempool when they are mined, and re-check unconfirmed transactions after confirming a blocks, etc.  However, this section pertains to expected p2p behavior to facilitate function of Kwil in the leader-based block production design.

### Stream Protocol Specs [WIP]

The above requirements necessitate a number of stream protocols. They will be enumerated and outlined here...

The protocol stream handlers and initiator methods will interact with several other higher level systems including: mempool, block store, block index, transaction index, consensus engine, etc. Systems such as RPC, instrumentation, etc. must not be involved.

Non-blocking: All stream handlers and initiators MUST NOT be blocking, and MUST be able to run concurrently. Other systems that interact with the P2P layer SHOULD utilize atomics, queues, goroutines, and other internal mechanisms for handling communications *asynchronously*.
