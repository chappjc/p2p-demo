package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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
	blk    block
	resCb  func(ack bool, appHash []byte) error
}

type block struct {
	ver      uint16
	prevHash Hash
	txids    []Hash
	txns     [][]byte
}

type consensusEngine struct {
	// bki *blockStore
	// mp *mempool
	// txi *transactionIndex

	leader atomic.Bool

	mtx        sync.RWMutex
	lastCommit blkCommit
	proposed   *blkProp
	// execRes    *blkExecResult

	// as leader, we collect acks for the proposed blocks
	acks map[peer.ID]ackRes
}

func (ce *consensusEngine) acceptProposalID(height int64, prevHash Hash) bool {
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

func (ce *consensusEngine) processProposal(blk []byte, height int64, prevHash Hash,
	res func(ack bool, appHash []byte) error) {
	if ce.leader.Load() {
		return
	}

	if ce.proposed != nil {
		fmt.Println("block proposal already exists")
		return
	}
	if height != ce.lastCommit.height+1 {
		log.Printf("proposal for height %d does not follow %d", height, ce.lastCommit.height)
		return
	}

	ver, encHeight, txids, txns, err := decodeBlock(blk)
	if err != nil {
		log.Printf("decodeBlock failed for proposal at height %d: %v", height, err)
		return
	}
	if encHeight != height {
		log.Printf("unexpected height: wanted %d, got %d", height, encHeight)
		return
	}

	block := block{
		ver:      ver,
		prevHash: prevHash,
		txids:    txids,
		txns:     txns,
	}

	blkHash := hashBlockHeader(ver, height, txids)

	// we will then begin execution, and later report with ack/nack

	ce.mtx.Lock()
	defer ce.mtx.Unlock()
	ce.proposed = &blkProp{
		height: height,
		hash:   Hash(blkHash),
		blk:    block,
		resCb:  res,
	}

	// OR

	// ce.evtChan <- *&blkProp{...}

	// ce event loop will send ACK+appHash or NACK.

	// ce should have some handle to p2p, like a function or channel into an
	// outgoing p2p msg loop.
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

	if !n.ce.acceptProposalID(height, prevHash) {
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

	ver, encHeight, txids, _, err := decodeBlock(blkProp)
	if err != nil {
		log.Printf("decodeBlock failed for proposal at height %d: %v", height, err)
		return
	}
	if encHeight != height {
		log.Printf("unexpected height: wanted %d, got %d", height, encHeight)
		return
	}

	blkHash := hashBlockHeader(ver, height, txids)
	var hash Hash
	copy(hash[:], blkHash)

	go n.ce.processProposal(blkProp, height, prevHash, func(ack bool, appHash []byte) error {
		return n.sendACK(ack, hash, appHash)
	})

	return
}

// sendACK is a callback for the result of validator block execution/precommit.
// After then consensus engine
func (n *Node) sendACK(ack bool, blkID Hash, appHash []byte) error {
	n.ackChan <- ackRes{
		ack:     ack,
		appHash: appHash,
		blkID:   blkID,
	}
	return nil // actually gossip the nack
}

type ackRes struct {
	ack     bool
	blkID   Hash
	appHash []byte
}

func (ar ackRes) ACK() string {
	if ar.ack {
		return "ACK"
	}
	return "nACK"
}

func (ar ackRes) String() string {
	if ar.ack {
		return fmt.Sprintf("%s: block %d, appHash %x", ar.ACK(), ar.blkID, ar.appHash)
	}
	return ar.ACK()
}

func (ar ackRes) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 1+len(ar.blkID)+4+len(ar.appHash))
	if ar.ack {
		buf[0] = 1
	}
	copy(buf[1:], ar.blkID[:])
	binary.LittleEndian.PutUint32(buf[1+len(ar.blkID):], uint32(len(ar.appHash)))
	copy(buf[1+len(ar.blkID)+4:], ar.appHash)
	return buf, nil
}

func (ar *ackRes) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("insufficient data")
	}
	ar.ack = data[0] == 1
	if !ar.ack {
		if len(data) > 1 {
			return fmt.Errorf("too much data for nACK")
		}
		ar.blkID = Hash{}
		ar.appHash = nil
		return nil
	}
	if len(data) < 1+len(ar.blkID)+4 {
		return fmt.Errorf("insufficient data for ACK")
	}
	copy(ar.blkID[:], data[1:1+len(ar.blkID)])
	appHashLen := binary.LittleEndian.Uint32(data[1+len(ar.blkID):])
	ar.appHash = make([]byte, appHashLen)
	copy(ar.appHash, data[1+len(ar.blkID)+4:])
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
		for ack := range n.ackChan {
			ackMsg, _ := ack.MarshalBinary()
			err := topicAck.Publish(ctx, ackMsg)
			if err != nil {
				fmt.Println("Publish:", err)
				return
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
				continue // ignore TODO just don't sub
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

			var ack ackRes
			err = ack.UnmarshalBinary(ackMsg.Data)
			if err != nil {
				log.Printf("failed to decode ACK msg: %v", err)
				continue
			}
			fromPeerID := ackMsg.GetFrom()

			log.Printf("received ACK msg from %v (rcvd from %s), data = %x",
				fromPeerID, ackMsg.ReceivedFrom, ackMsg.Message.Data)

			go n.ce.processACK(fromPeerID, ack)
		}
	}()

	return nil
}

func (ce *consensusEngine) processACK(from peer.ID, ack ackRes) {
	ce.mtx.Lock()
	defer ce.mtx.Unlock()
	_, have := ce.acks[from]
	if have {
		log.Printf("replacing known ACK from %v: %v", from, ack)
	}
	ce.acks[from] = ack
	// TODO: again, send to event loop, so this can trigger commit if threshold reached.
}

const (
	TopicACKs = "acks"
)

func subAcks(ctx context.Context, ps *pubsub.PubSub) (*pubsub.Topic, *pubsub.Subscription, error) {
	return subTopic(ctx, ps, TopicACKs)
}
