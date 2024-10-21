package node

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
)

type blkCommit struct {
	height int64
	hash   Hash
}

type blkProp struct {
	height int64
	hash   Hash
	blk    block
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

	mtx        sync.RWMutex
	lastCommit blkCommit
	proposed   *blkProp
	// execRes    *blkExecResult
}

func (ce *consensusEngine) acceptProposalID(height int64, prevHash Hash) bool {
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

func (ce *consensusEngine) processProposal(blk []byte, height int64, prevHash Hash) {
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

	ce.mtx.Lock()
	defer ce.mtx.Unlock()

	// we will then begin execution, and later report with ack/nack

	ce.proposed = &blkProp{
		height: height,
		hash:   Hash(blkHash),
		blk:    block,
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

	/*ver, encHeight, txids, _, err := decodeBlock(blkProp)
	if err != nil {
		log.Printf("decodeBlock failed for proposal at height %d: %v", height, err)
		return
	}
	if encHeight != height {
		log.Printf("unexpected height: wanted %d, got %d", height, encHeight)
		return
	}

	txHashes := make([][]byte, len(txids))
	for i := range txids {
		txHashes[i], err = hex.DecodeString(txids[i])
		if err != nil {
			log.Printf("decodeBlock failed for height %d: %v", height, err)
			return
		}
	}

	blkHash := hashBlockHeader(ver, height, txHashes)*/

	go n.ce.processProposal(blkProp, height, prevHash)

	return
}
