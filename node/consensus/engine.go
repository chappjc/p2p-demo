package consensus

import (
	"bytes"
	"fmt"
	"log"
	"p2p/node/types"
	"slices"
	"sync"
	"sync/atomic"
)

type blkCommit struct {
	height int64
	hash   types.Hash
}

type blkProp struct {
	height int64
	hash   types.Hash
	blk    *types.Block
	resCb  func(ack bool, appHash types.Hash) error
}

type block struct {
	ver      uint16
	prevHash types.Hash
	txids    []types.Hash
	txns     [][]byte
}

type Engine struct {
	bki types.BlockStore
	txi types.TxIndex
	mp  types.MemPool

	leader atomic.Bool

	mtx        sync.RWMutex
	lastCommit blkCommit
	proposed   *blkProp
	// execRes    *blkExecResult

	// as leader, we collect acks for the proposed blocks
	acks []ackFrom
}

func New(bs types.BlockStore, txi types.TxIndex, mp types.MemPool) *Engine {
	return &Engine{
		bki: bs,
		txi: txi,
		mp:  mp,
	}
}

type ackFrom struct {
	fromPubKey []byte
	res        types.AckRes
}

func (ce *Engine) AcceptProposalID(height int64, prevHash types.Hash) bool {
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
func (ce *Engine) ProcessProposal(blk *types.Block, res func(ack bool, appHash types.Hash) error) {
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
func (ce *Engine) AcceptCommit(height int64, blkHash types.Hash) (fetch bool) {
	if ce.proposed != nil && ce.proposed.hash == blkHash {
		// this should signal for CE to commit the block once it is executed.
		return false
	}
	if height != ce.lastCommit.height+1 {
		return false
	}
	return !ce.bki.Have(blkHash)
}

// CommitBlock reports a full block to commit. This would be used when:
//  1. retrieval of a block following in an announcement for a new+next block
//  2. iterative block retrieval in catch-up / sync
func (ce *Engine) CommitBlock(blk *types.Block, appHash types.Hash) error {
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
			blkHash, ce.proposed.hash, height)
	}

	// TODO: flag OK to commit ce.proposed.blk (with expected apphash)

	return nil
}

func (ce *Engine) confirmBlkTxns(blk *types.Block) {
	blkHash := blk.Header.Hash()
	height := blk.Header.Height

	log.Printf("confirming %d transactions in block %d (%v)", len(blk.Txns), height, blkHash)
	for _, txn := range blk.Txns {
		txHash := types.HashBytes(txn)
		ce.txi.Store(txHash, txn) // add to tx index
		ce.mp.Store(txHash, nil)  // rm from mempool
	}

	rawBlk := types.EncodeBlock(blk)
	ce.bki.Store(blkHash, height, rawBlk)
}

func (ce *Engine) ProcessACK(fromPubKey []byte, ack types.AckRes) {
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
