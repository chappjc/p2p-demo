package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type blockIndex struct {
	mtx      sync.RWMutex
	blkids   map[string]int64
	fetching map[string]bool
}

func newBlockIndex() *blockIndex {
	return &blockIndex{
		blkids:   make(map[string]int64),
		fetching: make(map[string]bool),
	}
}

func (bki *blockIndex) have(blkid string) bool { // this is racy
	bki.mtx.RLock()
	defer bki.mtx.RUnlock()
	_, have := bki.blkids[blkid]
	return have
}

func (bki *blockIndex) store(blkid string, height int64) {
	bki.mtx.Lock()
	defer bki.mtx.Unlock()
	if height == -1 {
		delete(bki.blkids, blkid)
		delete(bki.fetching, blkid)
		return
	}
	bki.blkids[blkid] = height
}

func (bki *blockIndex) preFetch(blkid string) bool {
	bki.mtx.Lock()
	defer bki.mtx.Unlock()
	if _, have := bki.blkids[blkid]; have {
		return false // don't need it
	}

	if fetching := bki.fetching[blkid]; fetching {
		return false // already getting it
	}
	bki.fetching[blkid] = true

	return true // go get it
}

func (bki *blockIndex) size() int {
	bki.mtx.RLock()
	defer bki.mtx.RUnlock()
	return len(bki.blkids)
}

func (bki *blockIndex) getBlk(blkid string) int64 {
	bki.mtx.RLock()
	defer bki.mtx.RUnlock()
	h, have := bki.blkids[blkid]
	if !have {
		return -1
	}
	return h
}

const (
	blkReadLimit  = 300_000_000
	blkGetTimeout = 90 * time.Second
)

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
	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		log.Printf("invalid height in blk ann request: %v", err)
		return
	}
	if height < 0 {
		log.Println("invalid height in blk ann request: %d", height)
		return
	}
	log.Printf("blk announcement received: %q / %d", blkid, height)

	if !n.bki.preFetch(blkid) {
		return // we have or are currently fetching it, do nothing, assuming we have already re-announced
	}

	log.Printf("retrieving new block: %q", blkid)

	// First try to get from this stream.
	rawBlk, err := request(s, []byte(getMsg), blkReadLimit)
	if err != nil {
		log.Printf("announcer failed to provide %v, trying other peers", blkid)
		// Since we are aware, ask other peers. we could also put this in a goroutine
		s.Close() // close the announcers stream first
		rawBlk, err = n.getBlkWithRetry(ctx, blkid, 500*time.Millisecond, 10)
		if err != nil {
			n.bki.store(blkid, height) // no longer fetching
			log.Printf("unable to retrieve tx %v: %v", blkid, err)
			return
		}
	}

	log.Println("obtained content for block", blkid)

	n.bki.store(blkid, int64(height))

	// re-announce

	go n.announceBlk(context.Background(), blkid, rawBlk, s.Conn().RemotePeer())
}

func (n *Node) announceBlk(ctx context.Context, blkid string, rawBlk []byte, from peer.ID) {
	peers := n.peers()
	if len(peers) == 0 {
		log.Println("no peers to advertise block to")
		return
	}

	for _, peerID := range peers {
		if peerID == from {
			continue
		}
		log.Printf("advertising block %v (height %d) to peer %v", blkid, len(rawBlk), peerID)
		resID := annBlkMsgPrefix + blkid
		err := advertiseToPeer(ctx, n.host, peerID, ProtocolIDBlkAnn, resID, rawBlk)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func (n *Node) getBlkWithRetry(ctx context.Context, blkid string, baseDelay time.Duration,
	maxAttempts int) ([]byte, error) {
	var attempts int
	for {
		raw, err := n.getBlk(ctx, blkid)
		if err == nil {
			return raw, nil
		}

		log.Printf("unable to retrieve block %v (%v), waiting to retry", blkid, err)

		select {
		case <-ctx.Done():
		case <-time.After(baseDelay):
		}
		baseDelay *= 2
		attempts++
		if attempts >= maxAttempts {
			return nil, ErrBlkNotFound
		}
	}
}

func (n *Node) getBlk(ctx context.Context, blkid string) ([]byte, error) {
	for _, peer := range n.peers() {
		log.Printf("requesting block %v from %v", blkid, peer)
		resID := getBlkMsgPrefix + blkid
		raw, err := requestFrom(ctx, n.host, peer, resID, blkReadLimit)
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
		return raw, nil
	}
	return nil, ErrBlkNotFound
}
