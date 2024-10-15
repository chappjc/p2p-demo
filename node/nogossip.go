package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (n *Node) txAnnStreamHandler(s network.Stream) {
	defer s.Close()

	s.SetDeadline(time.Now().Add(txGetTimeout))

	req := make([]byte, 128)
	nr, err := s.Read(req)
	if err != nil && err != io.EOF {
		log.Println("bad get tx req", err)
		return
	}
	req, ok := bytes.CutPrefix(req[:nr], []byte(annTxMsgPrefix))
	if !ok {
		log.Println("bad get tx request")
		return
	}
	txid := string(req)
	log.Printf("tx announcement received: %q", txid)

	if !n.txi.preFetch(txid) {
		return // we have or are currently fetching it, do nothing, assuming we have already re-announced
	} // now we must n.txi.storeTx(txid, ...)
	// TODO: distinguish mempool vs block store txns

	t0 := time.Now()
	// log.Printf("retrieving new tx: %q", txid)

	// First try to get from this stream.
	rawTx, err := requestTx(s, []byte(getMsg))
	if err != nil {
		log.Printf("announcer failed to provide %v, trying other peers", txid)
		// Since we are aware, ask other peers. we could also put this in a goroutine
		s.Close() // close the announcers stream first
		rawTx, err = n.getTxWithRetry(context.TODO(), txid, 500*time.Millisecond, 10)
		if err != nil {
			n.txi.storeTx(txid, nil) // no longer fetching
			log.Printf("unable to retrieve tx %v: %v", txid, err)
			return
		}
	}

	log.Printf("obtained content for tx %q in %v", txid, time.Since(t0))

	n.txi.storeTx(txid, rawTx)

	// re-announce

	go n.announceTx(context.Background(), txid, rawTx, s.Conn().RemotePeer())
}

func (n *Node) announceTx(ctx context.Context, txid string, rawTx []byte, from peer.ID) {
	peers := n.host.Network().Peers()
	if len(peers) == 0 {
		log.Println("no peers to advertise tx to")
		return
	}

	for _, peerID := range peers {
		if peerID == from {
			continue
		}
		log.Printf("advertising tx %v (len %d) to peer %v", txid, len(rawTx), peerID)
		err := n.advertiseTxToPeer(ctx, peerID, txid, rawTx)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

// advertiseTxToPeer sends a lightweight advertisement to a connected peer.
// The stream remains open in case the peer wants to request the content right.
func (n *Node) advertiseTxToPeer(ctx context.Context, peerID peer.ID, txid string, rawTx []byte) error {
	s, err := n.host.NewStream(ctx, peerID, ProtocolIDTxAnn)
	if err != nil {
		return fmt.Errorf("failed to open stream to peer: %w", err)
	}

	roundTripDeadline := time.Now().Add(3 * time.Second)
	s.SetWriteDeadline(roundTripDeadline)

	// Send a lightweight advertisement with the object ID
	_, err = s.Write([]byte(annTxMsgPrefix + txid))
	if err != nil {
		return fmt.Errorf("txann failed: %w", err)
	}

	log.Printf("advertised content %s to peer %s", txid, peerID)

	// Keep the stream open for potential content requests
	go func() {
		defer s.Close()

		s.SetReadDeadline(roundTripDeadline)

		req := make([]byte, 128)
		n, err := s.Read(req)
		if err != nil && err != io.EOF {
			log.Println("bad get tx req", err)
			return
		}
		req, ok := bytes.CutPrefix(req[:n], []byte(getMsg))
		if !ok {
			log.Println("bad get tx request")
			return
		}

		s.SetWriteDeadline(time.Now().Add(20 * time.Second))
		s.Write(rawTx)
	}()

	return nil
}

// startTxAnns creates pretend transactions, adds them to the tx index, and
// announces them to peers.
func (n *Node) startTxAnns(ctx context.Context, period time.Duration, sz int) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(period):
			}

			txid := hex.EncodeToString(randBytes(32))
			rawTx := randBytes(sz)
			n.txi.storeTx(txid, rawTx)

			log.Printf("announcing txid %v", txid)
			n.announceTx(ctx, txid, rawTx, n.host.ID())
		}
	}()
}
