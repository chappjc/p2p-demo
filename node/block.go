package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type blockStore struct {
	mtx      sync.RWMutex
	idx      map[string]int64
	fetching map[string]bool

	memStore map[string][]byte // TODO: disk
	db       *badger.DB        // <-disk
}

func newBlockStore(dir string) (*blockStore, error) {
	opts := badger.DefaultOptions(dir)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &blockStore{
		idx:      make(map[string]int64),
		fetching: make(map[string]bool),
		memStore: make(map[string][]byte),
		db:       db,
	}, nil
}

func (bki *blockStore) have(blkid string) bool { // this is racy
	bki.mtx.RLock()
	defer bki.mtx.RUnlock()
	_, have := bki.idx[blkid]
	return have
}

func (bki *blockStore) store(blkid string, height int64, raw []byte) {
	bki.mtx.Lock()
	defer bki.mtx.Unlock()
	delete(bki.fetching, blkid)
	if height == -1 {
		delete(bki.idx, blkid)
		return
	}
	bki.idx[blkid] = height

	bki.memStore[blkid] = raw
	err := bki.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("blk:"+blkid), raw)
	})
	if err != nil {
		panic(err)
	}
}

func (bki *blockStore) preFetch(blkid string) bool {
	bki.mtx.Lock()
	defer bki.mtx.Unlock()
	if _, have := bki.idx[blkid]; have {
		return false // don't need it
	}

	if fetching := bki.fetching[blkid]; fetching {
		return false // already getting it
	}
	bki.fetching[blkid] = true

	return true // go get it
}

func (bki *blockStore) size() int {
	bki.mtx.RLock()
	defer bki.mtx.RUnlock()
	return len(bki.idx)
}

func (bki *blockStore) getBlk(blkid string) (int64, []byte) {
	bki.mtx.RLock()
	defer bki.mtx.RUnlock()
	h, have := bki.idx[blkid]
	if !have {
		return -1, nil
	}
	// raw, have := bki.memStore[blkid]
	// if !have {
	// 	return -1, nil
	// }
	var raw []byte
	err := bki.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("blk:" + blkid))
		if err != nil {
			return err
		}
		raw, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		panic(err)
	}
	return h, raw
}

const (
	blkReadLimit  = 300_000_000
	blkGetTimeout = 90 * time.Second
)

func (n *Node) blkGetStreamHandler(s network.Stream) {
	defer s.Close()

	req := make([]byte, 128)
	// io.ReadFull(s, req)
	nr, err := s.Read(req)
	if err != nil && err != io.EOF {
		fmt.Println("bad get blk req", err)
		return
	}
	req, ok := bytes.CutPrefix(req[:nr], []byte(getBlkMsgPrefix))
	if !ok {
		fmt.Println("bad get blk request")
		return
	}
	blkid := strings.TrimSpace(string(req))
	log.Printf("requested blkid: %q", blkid)

	height, rawBlk := n.bki.getBlk(blkid)
	if height == -1 {
		s.Write(noData) // don't have it
	} else {
		binary.Write(s, binary.LittleEndian, height)
		s.Write(rawBlk)
	}
}

func (n *Node) blkAnnStreamHandler(s network.Stream) {
	defer s.Close()

	s.SetDeadline(time.Now().Add(blkGetTimeout))
	ctx, cancel := context.WithTimeout(context.Background(), blkGetTimeout)
	defer cancel()

	req := make([]byte, 128)
	nr, err := s.Read(req)
	if err != nil && err != io.EOF {
		log.Println("bad blk ann req", err)
		return
	}
	req, ok := bytes.CutPrefix(req[:nr], []byte(annBlkMsgPrefix))
	if !ok {
		log.Println("bad blk ann request")
		return
	}
	if len(req) <= 64 {
		log.Println("short blk ann request")
		return
	}
	blkid, after, cut := strings.Cut(string(req), ":")
	if !cut {
		log.Println("invalid blk ann request")
		return
	}
	var appHash Hash
	heightStr, appHashStr, cut := strings.Cut(after, ":")
	if cut {
		appHash, err = NewHashFromString(appHashStr)
		if err != nil {
			log.Println("appHash in blk ann request")
			return
		}
	}

	blkHash, err := NewHashFromString(blkid)
	if err != nil {
		log.Printf("invalid block id: %v", err)
		return
	}
	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		log.Printf("invalid height in blk ann request: %v", err)
		return
	}
	if height < 0 {
		log.Printf("invalid height in blk ann request: %d", height)
		return
	}
	log.Printf("blk announcement received: %q / %d", blkid, height)

	// If we are a validator and this is the commit ann for a proposed block tha
	// twe already started executing, consensus engine will handle it.
	// if !n.ce.AcceptCommit(height, blkHash) {
	// 	return
	// }

	// Possibly ce will handle it regardless.  For now, below is block store
	// code like a sentry node might do.

	if !n.bki.preFetch(blkid) {
		return // we have or are currently fetching it, do nothing, assuming we have already re-announced
	}
	var success bool
	defer func() {
		if !success { // did not get the rawTx
			n.bki.store(blkid, -1, nil) // no longer fetching
		}
	}()

	log.Printf("retrieving new block: %q", blkid)
	t0 := time.Now()

	// First try to get from this stream.
	rawBlk, err := request(s, []byte(getMsg), blkReadLimit)
	if err != nil {
		log.Printf("announcer failed to provide %v, trying other peers", blkid)
		// Since we are aware, ask other peers. we could also put this in a goroutine
		s.Close() // close the announcers stream first
		var gotHeight int64
		gotHeight, rawBlk, err = n.getBlkWithRetry(ctx, blkid, 500*time.Millisecond, 10)
		if err != nil {
			log.Printf("unable to retrieve tx %v: %v", blkid, err)
			return
		}
		if gotHeight != height {
			log.Printf("getblk response had unexpected height: wanted %d, got %d", height, gotHeight)
			return
		}
	}

	log.Printf("obtained content for block %q in %v", blkid, time.Since(t0))

	blk, err := DecodeBlock(rawBlk)
	if err != nil {
		log.Printf("decodeBlock failed for %v: %v", blkid, err)
		return
	}
	if blk.Header.Height != height {
		log.Printf("getblk response had unexpected height: wanted %d, got %d", height, blk.Header.Height)
		return
	}
	gotBlkHash := blk.Header.Hash()
	if gotBlkHash != blkHash {
		log.Printf("invalid block hash: wanted %v, got %x", blkHash, gotBlkHash)
		return
	}

	success = true

	// re-announce

	go func() {
		if err := n.ce.CommitBlock(blk, appHash); err != nil {
			log.Printf("cannot commit announced block: %v", err)
			return
		}
		n.announceBlk(context.Background(), blkid, height, rawBlk, s.Conn().RemotePeer())
	}()
}

func (n *Node) announceBlk(ctx context.Context, blkid string, height int64, rawBlk []byte, from peer.ID) {
	peers := n.peers()
	if len(peers) == 0 {
		log.Println("no peers to advertise block to")
		return
	}

	for _, peerID := range peers {
		if peerID == from {
			continue
		}
		log.Printf("advertising block %s (height %d / txs %d) to peer %v", blkid, height, len(rawBlk), peerID)
		resID := annBlkMsgPrefix + blkid + ":" + strconv.Itoa(int(height))
		err := advertiseToPeer(ctx, n.host, peerID, ProtocolIDBlkAnn, resID, rawBlk)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func (n *Node) getBlkWithRetry(ctx context.Context, blkid string, baseDelay time.Duration,
	maxAttempts int) (int64, []byte, error) {
	var attempts int
	for {
		height, raw, err := n.getBlk(ctx, blkid)
		if err == nil {
			return height, raw, nil
		}

		log.Printf("unable to retrieve block %v (%v), waiting to retry", blkid, err)

		select {
		case <-ctx.Done():
		case <-time.After(baseDelay):
		}
		baseDelay *= 2
		attempts++
		if attempts >= maxAttempts {
			return 0, nil, ErrBlkNotFound
		}
	}
}

func (n *Node) getBlk(ctx context.Context, blkid string) (int64, []byte, error) {
	for _, peer := range n.peers() {
		log.Printf("requesting block %v from %v", blkid, peer)
		t0 := time.Now()
		resID := getBlkMsgPrefix + blkid
		resp, err := requestFrom(ctx, n.host, peer, resID, ProtocolIDBlock, blkReadLimit)
		if errors.Is(err, ErrNotFound) {
			log.Printf("block not available on %v", peer)
			continue
		}
		if errors.Is(err, ErrNoResponse) {
			log.Printf("no response to block request to %v", peer)
			continue
		}
		if err != nil {
			log.Printf("unexpected error from %v: %v", peer, err)
			continue
		}

		if len(resp) < 8 {
			log.Printf("block response too short")
			continue
		}

		log.Printf("obtained content for block %q in %v", blkid, time.Since(t0))

		height := binary.LittleEndian.Uint64(resp[:8])
		rawBlk := resp[8:]

		return int64(height), rawBlk, nil
	}
	return 0, nil, ErrBlkNotFound
}

func NewBlock(ver uint16, height int64, prevHash, appHash Hash, stamp time.Time, txns [][]byte) *Block {
	numTxns := len(txns)
	txHashes := make([]Hash, numTxns)
	for i := range txns {
		txHashes[i] = HashBytes(txns[i])
	}
	merkelRoot := CalcMerkleRoot(txHashes)
	hdr := &BlockHeader{
		Version:     ver,
		Height:      height,
		PrevHash:    prevHash,
		PrevAppHash: appHash,
		NumTxns:     uint32(numTxns),
		Timestamp:   stamp.UTC(),
		MerkelRoot:  merkelRoot,
	}
	return &Block{
		Header: hdr,
		Txns:   txns,
	}
}

type BlockHeader struct {
	Version     uint16
	Height      int64
	NumTxns     uint32
	PrevHash    Hash // previous block's hash
	PrevAppHash Hash // app hash after last block
	Timestamp   time.Time
	MerkelRoot  Hash // Merkle tree reference to hash of all transactions for the block

	// Proposer []byte should be leader, so probably pointless here

	// ValidatorUpdates []ValidatorUpdate
	// ConsensusUpdates []ConsensusUpdate

	// ChainStateHash Hash // if we want to keep the "application" hash separate from state of consensus engine
	PrevExecResultHash Hash
}

type Block struct {
	Header    *BlockHeader
	Txns      [][]byte
	Signature []byte // Signature is the block producer's signature (leader in our model)
}

func DecodeBlockHeader(r io.Reader) (*BlockHeader, error) {
	hdr := new(BlockHeader)

	if err := binary.Read(r, binary.LittleEndian, &hdr.Version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &hdr.Height); err != nil {
		return nil, fmt.Errorf("failed to read height: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &hdr.NumTxns); err != nil {
		return nil, fmt.Errorf("failed to read number of transactions: %w", err)
	}

	_, err := io.ReadFull(r, hdr.PrevHash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read previous block hash: %w", err)
	}

	_, err = io.ReadFull(r, hdr.PrevAppHash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read previous block hash: %w", err)
	}

	var timeStamp uint64
	if err := binary.Read(r, binary.LittleEndian, &timeStamp); err != nil {
		return nil, fmt.Errorf("failed to read number of transactions: %w", err)
	}
	hdr.Timestamp = time.UnixMilli(int64(timeStamp))

	_, err = io.ReadFull(r, hdr.MerkelRoot[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read merkel root: %w", err)
	}

	return hdr, nil
}

func (bh *BlockHeader) String() string {
	return fmt.Sprintf("BlockHeader{Version: %d, Height: %d, NumTxns: %d, PrevHash: %s, AppHash: %s, Timestamp: %s, MerkelRoot: %s}",
		bh.Version,
		bh.Height,
		bh.NumTxns,
		bh.PrevHash,
		bh.PrevAppHash,
		bh.Timestamp.Format(time.RFC3339),
		bh.MerkelRoot)
}

func (bh *BlockHeader) writeBlockHeader(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, bh.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	if err := binary.Write(w, binary.LittleEndian, bh.Height); err != nil {
		return fmt.Errorf("failed to write height: %w", err)
	}

	if err := binary.Write(w, binary.LittleEndian, bh.NumTxns); err != nil {
		return fmt.Errorf("failed to write number of transactions: %w", err)
	}

	if _, err := w.Write(bh.PrevHash[:]); err != nil {
		return fmt.Errorf("failed to write previous block hash: %w", err)
	}

	if _, err := w.Write(bh.PrevAppHash[:]); err != nil {
		return fmt.Errorf("failed to write app hash: %w", err)
	}

	if err := binary.Write(w, binary.LittleEndian, uint64(bh.Timestamp.UnixMilli())); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}

	if _, err := w.Write(bh.MerkelRoot[:]); err != nil {
		return fmt.Errorf("failed to write merkel root: %w", err)
	}

	return nil
}

func EncodeBlockHeader(hdr *BlockHeader) []byte {
	var buf bytes.Buffer
	hdr.writeBlockHeader(&buf)
	return buf.Bytes()
}

func (bh *BlockHeader) Hash() Hash {
	hasher := sha256.New()
	bh.writeBlockHeader(hasher)
	return Hash(hasher.Sum(nil))
}

/*func encodeBlockHeaderOneAlloc(hdr *BlockHeader) []byte {
	// Preallocate buffer: 2 + 8 + 4 + 32 + 32 + 8 + 32 = 118 bytes
	buf := make([]byte, 0, 118)

	buf = binary.LittleEndian.AppendUint16(buf, hdr.Version)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(hdr.Height))
	buf = binary.LittleEndian.AppendUint32(buf, hdr.NumTxns)
	buf = append(buf, hdr.PrevHash[:]...)
	buf = append(buf, hdr.AppHash[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(hdr.Timestamp.UnixMilli()))
	buf = append(buf, hdr.MerkelRoot[:]...)

	return buf
}*/

func EncodeBlock(block *Block) []byte {
	headerBytes := EncodeBlockHeader(block.Header)

	totalSize := len(headerBytes)
	for _, tx := range block.Txns {
		totalSize += 4 + len(tx) // 4 bytes for transaction length
	}

	buf := make([]byte, 0, totalSize)

	buf = append(buf, headerBytes...)

	for _, tx := range block.Txns {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(tx)))
		buf = append(buf, tx...)
	}

	return buf
}

// CalcMerkleRoot computes the merkel root for a slice of hashes. This is based
// on the "inline" implementation from btcd / dcrd.
func CalcMerkleRoot(leaves []Hash) Hash {
	if len(leaves) == 0 {
		// All zero.
		return Hash{}
	}

	// Do not modify the leaves slice from the caller.
	leaves = slices.Clone(leaves)

	// Create a buffer to reuse for hashing the branches and some long lived
	// slices into it to avoid reslicing.
	var buf [2 * HashLen]byte
	var left = buf[:HashLen]
	var right = buf[HashLen:]
	var both = buf[:]

	// The following algorithm works by replacing the leftmost entries in the
	// slice with the concatenations of each subsequent set of 2 hashes and
	// shrinking the slice by half to account for the fact that each level of
	// the tree is half the size of the previous one.  In the case a level is
	// unbalanced (there is no final right child), the final node is duplicated
	// so it ultimately is concatenated with itself.
	//
	// For example, the following illustrates calculating a tree with 5 leaves:
	//
	// [0 1 2 3 4]                              (5 entries)
	// 1st iteration: [h(0||1) h(2||3) h(4||4)] (3 entries)
	// 2nd iteration: [h(h01||h23) h(h44||h44)] (2 entries)
	// 3rd iteration: [h(h0123||h4444)]         (1 entry)
	for len(leaves) > 1 {
		// When there is no right child, the parent is generated by hashing the
		// concatenation of the left child with itself.
		if len(leaves)&1 != 0 {
			leaves = append(leaves, leaves[len(leaves)-1])
		}

		// Set the parent node to the hash of the concatenation of the left and
		// right children.
		for i := 0; i < len(leaves)/2; i++ {
			copy(left, leaves[i*2][:])
			copy(right, leaves[i*2+1][:])
			leaves[i] = sha256.Sum256(both)
		}
		leaves = leaves[:len(leaves)/2]
	}
	return leaves[0]
}

func DecodeBlock(rawBlk []byte) (*Block, error) {
	r := bytes.NewReader(rawBlk)

	hdr, err := DecodeBlockHeader(r)
	if err != nil {
		return nil, err
	}

	txns := make([][]byte, hdr.NumTxns)

	for i := range txns {
		var txLen uint32
		if err := binary.Read(r, binary.LittleEndian, &txLen); err != nil {
			return nil, fmt.Errorf("failed to read tx length: %w", err)
		}

		if int(txLen) > len(rawBlk) { // TODO: do better than this
			return nil, fmt.Errorf("invalid transaction length %d", txLen)
		}

		tx := make([]byte, txLen)
		if _, err := io.ReadFull(r, tx); err != nil {
			return nil, fmt.Errorf("failed to read tx data: %w", err)
		}
		txns[i] = tx
	}

	return &Block{
		Header: hdr,
		Txns:   txns,
	}, nil
}

/*func decodeBlockTxns(raw []byte) (txns [][]byte, err error) {
	rd := bytes.NewReader(raw)
	for rd.Len() > 0 {
		var txLen uint64
		if err := binary.Read(rd, binary.LittleEndian, &txLen); err != nil {
			return nil, fmt.Errorf("failed to read tx length: %w", err)
		}

		if txLen > uint64(rd.Len()) {
			return nil, fmt.Errorf("invalid tx length %d", txLen)
		}
		tx := make([]byte, txLen)
		if _, err := io.ReadFull(rd, tx); err != nil {
			return nil, fmt.Errorf("failed to read tx data: %w", err)
		}
		txns = append(txns, tx)
	}

	return txns, nil
}*/
