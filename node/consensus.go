package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type blkCommit struct {
	height int64
	hash   Hash
}

type blkProp struct {
	height int64
	hash   Hash
	blk    *Block
	resCb  func(ack bool, appHash Hash) error
}

type block struct {
	ver      uint16
	prevHash Hash
	txids    []Hash
	txns     [][]byte
}

type consensusEngine struct {
	bki *blockStore
	txi *transactionIndex
	mp  *mempool

	leader atomic.Bool

	mtx        sync.RWMutex
	lastCommit blkCommit
	proposed   *blkProp
	// execRes    *blkExecResult

	// as leader, we collect acks for the proposed blocks
	acks []ackFrom
}

type ackFrom struct {
	fromPubKey []byte
	res        AckRes
}

func (ce *consensusEngine) AcceptProposalID(height int64, prevHash Hash) bool {
	if ce.leader.Load() {
		return false
	}

	ce.mtx.RLock()
	defer ce.mtx.RUnlock()
	if ce.proposed != nil {
		fmt.Println("block proposal already exists")
		return false
	}
	// initial block must precede genesis
	if height == 1 || prevHash.IsZero() {
		return ce.lastCommit.hash.IsZero()
	}

	if height != ce.lastCommit.height+1 {
		return false
	}
	return prevHash == ce.lastCommit.hash
}

// ProcessProposal handles a full block proposal from the leader. Only a
// validator should use this method, not leader or sentry. This validates the
// proposal, ensuring that it is for the next block (by height and previous
// block hash), and beings executing the block. When execution is complete, the
// res callback function is called with ACK+appHash/nACK, which in the context
// of the node will send the outcome back to the leader where validator
// responses are tallied.
func (ce *consensusEngine) ProcessProposal(blk *Block, res func(ack bool, appHash Hash) error) {
	if ce.leader.Load() {
		return
	}

	ce.mtx.Lock()
	defer ce.mtx.Unlock()

	if ce.proposed != nil {
		fmt.Println("block proposal already exists")
		return
	}
	if blk.Header.Height != ce.lastCommit.height+1 {
		log.Printf("proposal for height %d does not follow %d", blk.Header.Height, ce.lastCommit.height)
		return
	}

	blkHash := blk.Header.Hash()

	// we will then begin execution, and later report with ack/nack

	ce.proposed = &blkProp{
		height: blk.Header.Height,
		hash:   blkHash,
		blk:    blk,
		resCb:  res,
	}

	// OR

	// ce.evtChan <- *&blkProp{...}

	// ce event loop will send ACK+appHash or NACK.

	// ce should have some handle to p2p, like a function or channel into an
	// outgoing p2p msg loop.
}

// AcceptCommit is used for a validator to handle a committed block
// announcement. The result should indicate if if the block should be fetched.
// This will return false when ANY of the following are the case:
//
//  1. (validator) we had the proposed block, which we will commit when ready
//  2. this is not the next block in our local store
//
// This will return true if we should fetch the block, which is the case if BOTH
// we did not have a proposal for this block, and it is the next in our block store.
func (ce *consensusEngine) AcceptCommit(height int64, blkHash Hash) (fetch bool) {
	if ce.proposed != nil && ce.proposed.hash == blkHash {
		// this should signal for CE to commit the block once it is executed.
		return false
	}
	if height != ce.lastCommit.height+1 {
		return false
	}
	blkID := blkHash.String()
	return !ce.bki.have(blkID)
}

// CommitBlock reports a full block to commit. This would be used when:
//  1. retrieval of a block following in an announcement for a new+next block
//  2. iterative block retrieval in catch-up / sync
func (ce *consensusEngine) CommitBlock(blk *Block, appHash Hash) error {
	height := blk.Header.Height
	if ce.proposed == nil {
		// execute and commit if it is next
		if height != ce.lastCommit.height+1 {
			return fmt.Errorf("block at height %d does not follow %d", height, ce.lastCommit.height)
		}
		// TODO: execute and commit
		return nil
	}

	if ce.proposed.height != height {
		return fmt.Errorf("block at height %d does not match existing proposed block at %d", height, ce.proposed.height)
	}

	blkHash := blk.Header.Hash()
	if ce.proposed.hash != blkHash {
		return fmt.Errorf("block at height %d with hash %v does not match hash of existing proposed block %d",
			blkHash, ce.proposed.hash)
	}

	// TODO: flag OK to commit ce.proposed.blk (with expected apphash)

	return nil
}

func (ce *consensusEngine) confirmBlkTxns(blk *Block) {
	blkid := blk.Header.Hash().String()
	height := blk.Header.Height

	log.Printf("confirming %d transactions in block %d (%v)", len(blk.Txns), height, blkid)
	for _, txn := range blk.Txns {
		txHash := HashBytes(txn)
		txid := txHash.String()
		ce.txi.storeTx(txid, txn) // add to tx index
		ce.mp.storeTx(txid, nil)  // rm from mempool
	}

	rawBlk := EncodeBlock(blk)
	ce.bki.store(blkid, height, rawBlk)
}

// blkPropStreamHandler is the stream handler for the ProtocolIDBlockPropose
// protocol i.e. proposed block announcements, which originate from the leader,
// but may be re-announced by other validators.
//
// This stream should:
//  1. provide the announcement to the consensus engine (CE)
//  2. if the CE rejects the ann, close stream
//  3. if the CE is ready for this proposed block, request the block
//  4. provide the block contents to the CE
//  5. close the stream
//
// Note that CE decides what to do. For instance, after we provide the full
// block contents, the CE will likely begin executing the blocks. When it is
// done, it will send an ACK/NACK with the
func (n *Node) blkPropStreamHandler(s network.Stream) {
	defer s.Close()

	if n.leader.Load() {
		return
	}

	// "prop:height:prevHash"
	propID := make([]byte, 128)
	nr, err := s.Read(propID)
	if err != nil {
		log.Println("failed to read block proposal ID:", err)
		return
	}
	propID, ok := bytes.CutPrefix(propID[nr:], []byte(annPropMsgPrefix))
	if !ok {
		log.Println("bad block proposal ID:", propID)
		return
	}
	heightStr, prevBlkID, ok := bytes.Cut(propID, []byte(":"))
	if !ok {
		log.Println("bad block proposal ID:", propID)
		return
	}
	height, err := strconv.ParseInt(string(heightStr), 10, 64)
	if err != nil {
		log.Println("invalid height in proposal ID:", err)
		return
	}
	prevHash, err := NewHashFromString(string(prevBlkID))
	if err != nil {
		log.Println("invalid prev block hash in proposal ID:", err)
		return
	}

	if !n.ce.AcceptProposalID(height, prevHash) {
		return
	}

	nr, err = s.Write([]byte(getMsg))
	if err != nil {
		log.Println("failed to request block proposal contents:", err)
		return
	}

	rd := bufio.NewReader(s)
	blkProp, err := io.ReadAll(rd)
	if err != nil {
		log.Println("failed to read block proposal contents:", err)
		return
	}

	// Q: header first, or full serialized block?

	blk, err := DecodeBlock(blkProp)
	if err != nil {
		log.Printf("decodeBlock failed for proposal at height %d: %v", height, err)
		return
	}
	if blk.Header.Height != height {
		log.Printf("unexpected height: wanted %d, got %d", height, blk.Header.Height)
		return
	}

	hash := blk.Header.Hash()

	go n.ce.ProcessProposal(blk, func(ack bool, appHash Hash) error {
		return n.sendACK(ack, hash, appHash)
	})

	return
}

// sendACK is a callback for the result of validator block execution/precommit.
// After then consensus engine executes the block, this is used to gossip the
// result back to the leader.
func (n *Node) sendACK(ack bool, blkID Hash, appHash Hash) error {
	n.ackChan <- AckRes{
		ack:     ack,
		appHash: appHash,
		blkID:   blkID,
	}
	return nil // actually gossip the nack
}

type AckRes struct {
	ack     bool
	blkID   Hash
	appHash Hash
}

func (ar AckRes) ACK() string {
	if ar.ack {
		return "ACK"
	}
	return "nACK"
}

func (ar AckRes) String() string {
	if ar.ack {
		return fmt.Sprintf("%s: block %d, appHash %x", ar.ACK(), ar.blkID, ar.appHash)
	}
	return ar.ACK()
}

func (ar AckRes) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 1+len(ar.blkID)+len(ar.appHash))
	if ar.ack {
		buf[0] = 1
	}
	copy(buf[1:], ar.blkID[:])
	copy(buf[1+len(ar.blkID)+4:], ar.appHash[:])
	return buf, nil
}

func (ar *AckRes) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("insufficient data")
	}
	ar.ack = data[0] == 1
	if !ar.ack {
		if len(data) > 1 {
			return fmt.Errorf("too much data for nACK")
		}
		ar.blkID = Hash{}
		ar.appHash = Hash{}
		return nil
	}
	data = data[1:]
	if len(data) < len(ar.blkID)+len(ar.appHash) {
		return fmt.Errorf("insufficient data for ACK")
	}
	copy(ar.blkID[:], data[:len(ar.blkID)])
	copy(ar.appHash[:], data[len(ar.blkID):])
	return nil
}

func (n *Node) startAckGossip(ctx context.Context, ps *pubsub.PubSub) error {
	topicAck, subAck, err := subAcks(ctx, ps)
	if err != nil {
		return err
	}

	subCanceled := make(chan struct{})

	n.wg.Add(1)
	go func() {
		defer func() {
			<-subCanceled
			topicAck.Close()
			n.wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
			case ack := <-n.ackChan:
				ackMsg, _ := ack.MarshalBinary()
				err := topicAck.Publish(ctx, ackMsg)
				if err != nil {
					fmt.Println("Publish:", err)
					// TODO: queue the ack for retry (send back to ackChan or another delayed send queue)
					return
				}
			}

		}
	}()

	me := n.host.ID()

	go func() {
		defer close(subCanceled)
		defer subAck.Cancel()
		for {
			if !n.leader.Load() {
				subAck.Next(ctx)
				continue // discard, we are just relaying
			}

			ackMsg, err := subAck.Next(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Println("subTx.Next:", err)
				}
				return
			}

			if peer.ID(ackMsg.From) == me {
				// log.Println("message from me ignored")
				continue
			}

			var ack AckRes
			err = ack.UnmarshalBinary(ackMsg.Data)
			if err != nil {
				log.Printf("failed to decode ACK msg: %v", err)
				continue
			}
			fromPeerID := ackMsg.GetFrom()

			log.Printf("received ACK msg from %v (rcvd from %s), data = %x",
				fromPeerID, ackMsg.ReceivedFrom, ackMsg.Message.Data)

			peerPubKey, err := fromPeerID.ExtractPublicKey()
			if err != nil {
				log.Printf("failed to extract pubkey from peer ID %v: %v", fromPeerID, err)
				continue
			}
			pubkeyBytes, _ := peerPubKey.Raw() // does not error for secp256k1 or ed25519
			go n.ce.ProcessACK(pubkeyBytes, ack)
		}
	}()

	return nil
}

func (ce *consensusEngine) ProcessACK(fromPubKey []byte, ack AckRes) {
	ce.mtx.Lock()
	defer ce.mtx.Unlock()
	idx := slices.IndexFunc(ce.acks, func(r ackFrom) bool {
		return bytes.Equal(r.fromPubKey, fromPubKey)
	})
	af := ackFrom{fromPubKey, ack}
	if idx == -1 {
		ce.acks = append(ce.acks, af)
	} else {
		log.Printf("replacing known ACK from %x: %v", fromPubKey, ack)
		ce.acks[idx] = af
	}

	// TODO: again, send to event loop, so this can trigger commit if threshold reached.
}

const (
	TopicACKs = "acks"
)

func subAcks(ctx context.Context, ps *pubsub.PubSub) (*pubsub.Topic, *pubsub.Subscription, error) {
	return subTopic(ctx, ps, TopicACKs)
}
