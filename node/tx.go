package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	ErrNotFound    = errors.New("resource not available")
	ErrTxNotFound  = errors.New("tx not available")
	ErrBlkNotFound = errors.New("block not available")
	ErrNoResponse  = errors.New("stream closed without response")
)

const (
	txReadLimit  = 30_000_000
	txGetTimeout = 20 * time.Second
)

func readTxResp(rd io.Reader) ([]byte, error) {
	rd = io.LimitReader(rd, txReadLimit)
	resp, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, ErrNoResponse
	}
	if bytes.Equal(resp, []byte("0")) {
		return nil, ErrTxNotFound
	}
	return resp, nil
}

func getTx(ctx context.Context, txid string, peer peer.ID, host host.Host) ([]byte, error) {
	txStream, err := host.NewStream(ctx, peer, ProtocolIDGetTx)
	if err != nil {
		return nil, err
	}
	defer txStream.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(txGetTimeout)
	}

	txStream.SetDeadline(deadline)

	return requestTx(txStream, []byte(getTxMsgPrefix+txid))
}

func requestTx(rw io.ReadWriter, reqMsg []byte) ([]byte, error) {
	content, err := request(rw, reqMsg, txReadLimit)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrTxNotFound
		}
		return nil, fmt.Errorf("tx get request failed: %v", err)
	}
	return content, nil
}

func (n *Node) getTx(ctx context.Context, txid string) ([]byte, error) {
	for _, peer := range n.peers() {
		log.Printf("requesting tx %v from %v", txid, peer)
		raw, err := getTx(ctx, txid, peer, n.host)
		if errors.Is(err, ErrTxNotFound) {
			log.Printf("transaction not available on %v", peer)
			continue
		}
		if errors.Is(err, ErrNoResponse) {
			log.Printf("no response to tx request to %v", peer)
			continue
		}
		if err != nil {
			log.Printf("unexpected error from %v: %v", peer, err)
			continue
		}
		return raw, nil
	}
	return nil, ErrTxNotFound
}

func (n *Node) getTxWithRetry(ctx context.Context, txid string, baseDelay time.Duration,
	maxAttempts int) ([]byte, error) {
	var attempts int
	for {
		raw, err := n.getTx(ctx, txid)
		if err == nil {
			return raw, nil
		}
		log.Printf("unable to retrieve tx %v (%v), waiting to retry", txid, err)
		select {
		case <-ctx.Done():
		case <-time.After(baseDelay):
		}
		baseDelay *= 2
		attempts++
		if attempts >= maxAttempts {
			return nil, ErrTxNotFound
		}
	}
}

type transactionIndex struct {
	mtx      sync.RWMutex
	txids    map[string][]byte
	fetching map[string]bool
}

func newTransactionIndex() *transactionIndex {
	return &transactionIndex{
		txids:    make(map[string][]byte),
		fetching: make(map[string]bool),
	}
}

func (txi *transactionIndex) txGetStreamHandler(s network.Stream) {
	defer s.Close()

	req := make([]byte, 128)
	n, err := s.Read(req)
	if err != nil && err != io.EOF {
		fmt.Println("bad get tx req", err)
		return
	}
	req, ok := bytes.CutPrefix(req[:n], []byte(getTxMsgPrefix))
	if !ok {
		fmt.Println("bad get tx request")
		return
	}
	txid := string(req)
	log.Printf("requested txid: %q", txid)
	rawTx := txi.getTx(txid)
	if rawTx == nil {
		s.Write([]byte("0"))
	} else {
		s.Write(rawTx)
	}
}

func (txi *transactionIndex) have(txid string) bool { // this is racy
	txi.mtx.RLock()
	defer txi.mtx.RUnlock()
	_, have := txi.txids[txid]
	return have
}

func (txi *transactionIndex) storeTx(txid string, raw []byte) {
	txi.mtx.Lock()
	defer txi.mtx.Unlock()
	if raw == nil {
		delete(txi.txids, txid)
		delete(txi.fetching, txid)
		return
	}
	txi.txids[txid] = raw
}

func (txi *transactionIndex) preFetch(txid string) bool {
	txi.mtx.Lock()
	defer txi.mtx.Unlock()
	if _, have := txi.txids[txid]; have {
		return false // don't need it
	}

	if fetching := txi.fetching[txid]; fetching {
		return false // already getting it
	}
	txi.fetching[txid] = true

	return true // go get it
}

func (txi *transactionIndex) size() int {
	txi.mtx.RLock()
	defer txi.mtx.RUnlock()
	return len(txi.txids)
}

func (txi *transactionIndex) getTx(txid string) []byte {
	txi.mtx.RLock()
	defer txi.mtx.RUnlock()
	return txi.txids[txid]
}

func (txi *transactionIndex) reapN(n int) map[string][]byte {
	txi.mtx.Lock()
	defer txi.mtx.Unlock()
	out := make(map[string][]byte, min(n, len(txi.txids)))
	for txid, rawTx := range txi.txids {
		out[txid] = rawTx
		delete(txi.txids, txid)
	}
	return out
}
