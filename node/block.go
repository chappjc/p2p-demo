package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
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
	if height == -1 {
		delete(bki.idx, blkid)
		delete(bki.fetching, blkid)
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
	blkid, heightStr, cut := strings.Cut(string(req), ":")
	if !cut {
		log.Println("invalid blk ann request")
		return
	}
	blkHash, err := hex.DecodeString(blkid)
	if err != nil {
		log.Printf("invalid block id: %v", err)
		return
	}
	if len(blkHash) != 32 {
		log.Printf("invalid block id: len %d", len(blkHash))
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
	// if n.ce.commitProposed(blkid) {
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

	ver, encHeight, txids, txns, err := decodeBlock(rawBlk)
	if err != nil {
		log.Printf("decodeBlock failed for %v: %v", blkid, err)
		return
	}
	if encHeight != height {
		log.Printf("getblk response had unexpected height: wanted %d, got %d", height, encHeight)
		return
	}
	gotBlkHash := hashBlockHeader(ver, height, txids)
	if !bytes.Equal(gotBlkHash, blkHash) {
		log.Printf("invalid block hash: wanted %x, got %x", blkHash, gotBlkHash)
		return
	}

	success = true

	log.Printf("confirming %d transactions in block %d (%v)", len(txids), height, blkid)
	for i, txHash := range txids {
		txid := txHash.String()
		n.txi.storeTx(txid, txns[i]) // add to tx index
		n.mp.storeTx(txid, nil)      // rm from mempool
	}

	n.bki.store(blkid, height, rawBlk)

	// re-announce
	go n.announceBlk(context.Background(), blkid, height, rawBlk, s.Conn().RemotePeer())
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

// type TxHash [32]byte

func encodeBlock(height int64, txids []string, txns [][]byte) (string, []byte, error) {
	hasher := sha256.New()
	var buf bytes.Buffer // TODO: more efficient

	verBts := binary.LittleEndian.AppendUint16(nil, uint16(0))
	hasher.Write(verBts)
	buf.Write(verBts)

	heightBts := binary.LittleEndian.AppendUint64(nil, uint64(height))
	hasher.Write(heightBts)
	buf.Write(heightBts)

	numTxBts := binary.LittleEndian.AppendUint64(nil, uint64(len(txns)))
	hasher.Write(numTxBts)
	buf.Write(numTxBts)

	for i, txid := range txids {
		txhash, err := hex.DecodeString(txid)
		if err != nil {
			return "", nil, fmt.Errorf("invalid tx hash for block: %w", err)
		}
		buf.Write(txhash)
		hasher.Write(txhash)

		tx := txns[i] // TODO: range check
		buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(tx))))
		buf.Write(tx)
		// tx bytes are not in block hash
	}

	blkHash := hasher.Sum(nil)
	rawBlk := buf.Bytes()
	return hex.EncodeToString(blkHash), rawBlk, nil
}

/*func decodeBlock(rawBlk []byte) (version uint16, height int64, txids []string, txns [][]byte, err error) {
	if len(rawBlk) < 18 { // 2 (version) + 8 (height) + 8 (num txs)
		return 0, 0, nil, nil, fmt.Errorf("block data too short")
	}

	r := bytes.NewReader(rawBlk)

	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to read version: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &height); err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to read height: %w", err)
	}

	var numTx uint64
	if err := binary.Read(r, binary.LittleEndian, &numTx); err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to read number of transactions: %w", err)
	}

	txids = make([]string, numTx)
	txns = make([][]byte, numTx)

	for i := range numTx {
		txhash := make([]byte, 32)
		if _, err := io.ReadFull(r, txhash); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to read tx hash: %w", err)
		}
		txids[i] = hex.EncodeToString(txhash)

		var txLen uint64
		if err := binary.Read(r, binary.LittleEndian, &txLen); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to read tx length: %w", err)
		}

		tx := make([]byte, txLen)
		if _, err := io.ReadFull(r, tx); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to read tx data: %w", err)
		}
		txns[i] = tx
	}

	return version, height, txids, txns, nil
}*/

func decodeBlockHeader(r io.Reader) (version uint16, height int64, numTx uint64, txids []Hash, err error) {
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return 0, 0, 0, nil, fmt.Errorf("failed to read version: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &height); err != nil {
		return 0, 0, 0, nil, fmt.Errorf("failed to read height: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &numTx); err != nil {
		return 0, 0, 0, nil, fmt.Errorf("failed to read number of transactions: %w", err)
	}

	txids = make([]Hash, numTx)

	for i := uint64(0); i < numTx; i++ {
		txhash := make([]byte, 32)
		if _, err := io.ReadFull(r, txhash); err != nil {
			return 0, 0, 0, nil, fmt.Errorf("failed to read tx hash: %w", err)
		}
		txids[i], _ = NewHashFromBytes(txhash)
	}

	return version, height, numTx, txids, nil
}

func decodeBlockTxns(raw []byte) (txns [][]byte, err error) {
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
}

func decodeBlock(rawBlk []byte) (version uint16, height int64, txids []Hash, txns [][]byte, err error) {
	if len(rawBlk) < 18 { // 2 (version) + 8 (height) + 8 (num txs)
		return 0, 0, nil, nil, fmt.Errorf("block data too short")
	}

	r := bytes.NewReader(rawBlk)

	version, height, numTx, txids, err := decodeBlockHeader(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	txns = make([][]byte, numTx)

	for i := uint64(0); i < numTx; i++ {
		var txLen uint64
		if err := binary.Read(r, binary.LittleEndian, &txLen); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to read tx length: %w", err)
		}

		tx := make([]byte, txLen)
		if _, err := io.ReadFull(r, tx); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to read tx data: %w", err)
		}
		txns[i] = tx
	}

	return version, height, txids, txns, nil
}

func hashBlockHeader(ver uint16, height int64, txHashes []Hash) []byte {
	hasher := sha256.New()

	verBts := binary.LittleEndian.AppendUint16(nil, ver)
	hasher.Write(verBts)

	heightBts := binary.LittleEndian.AppendUint64(nil, uint64(height))
	hasher.Write(heightBts)

	numTxBts := binary.LittleEndian.AppendUint64(nil, uint64(len(txHashes)))
	hasher.Write(numTxBts)

	for _, txid := range txHashes {
		hasher.Write(txid[:])
	}

	return hasher.Sum(nil)
}

func hashBlockHeader2(ver uint16, height int64, txHashes []Hash) []byte {
	hasher := sha256.New()

	verBts := binary.LittleEndian.AppendUint16(nil, ver)
	hasher.Write(verBts)

	heightBts := binary.LittleEndian.AppendUint64(nil, uint64(height))
	hasher.Write(heightBts)

	numTxBts := binary.LittleEndian.AppendUint64(nil, uint64(len(txHashes)))
	hasher.Write(numTxBts)

	for _, txid := range txHashes {
		hasher.Write(txid[:])
	}

	return hasher.Sum(nil)
}
