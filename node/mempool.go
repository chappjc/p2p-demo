package node

import (
	"sync"
)

// mempool is an index of unconfirmed transactions

type mempool struct {
	mtx      sync.RWMutex
	txns     map[string][]byte
	fetching map[string]bool
}

func newMempool() *mempool {
	return &mempool{
		txns:     make(map[string][]byte),
		fetching: make(map[string]bool),
	}
}

func (mp *mempool) have(txid string) bool { // this is racy
	mp.mtx.RLock()
	defer mp.mtx.RUnlock()
	_, have := mp.txns[txid]
	return have
}

func (mp *mempool) storeTx(txid string, raw []byte) {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	if raw == nil {
		delete(mp.txns, txid)
		delete(mp.fetching, txid)
		return
	}
	mp.txns[txid] = raw
}

func (mp *mempool) preFetch(txid string) bool {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	if _, have := mp.txns[txid]; have {
		return false // don't need it
	}

	if fetching := mp.fetching[txid]; fetching {
		return false // already getting it
	}
	mp.fetching[txid] = true

	return true // go get it
}

func (mp *mempool) size() int {
	mp.mtx.RLock()
	defer mp.mtx.RUnlock()
	return len(mp.txns)
}

func (mp *mempool) getTx(txid string) []byte {
	mp.mtx.RLock()
	defer mp.mtx.RUnlock()
	return mp.txns[txid]
}

func (mp *mempool) reapN(n int) ([]string, [][]byte) {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	n = min(n, len(mp.txns))
	txids := make([]string, 0, n)
	txns := make([][]byte, 0, n)
	for txid, rawTx := range mp.txns {
		delete(mp.txns, txid)
		txids = append(txids, txid)
		txns = append(txns, rawTx)
	}
	return txids, txns
}
