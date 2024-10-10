package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type transactionIndex struct {
	txids map[string][]byte
}

func newTransactionIndex() *transactionIndex {
	return &transactionIndex{txids: make(map[string][]byte)}
}

func (txi *transactionIndex) txStreamHandler(s network.Stream) {
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
	// if len(req) != 64 {
	// 	fmt.Println("bad tx hash in request")
	// 	return
	// }
	txid := string(req)
	fmt.Printf("txid: %q\n", txid)
	rawTx, have := txi.txids[txid]
	if !have {
		s.Write([]byte("0"))
	} else {
		s.Write(rawTx)
	}
}

func getTx(ctx context.Context, txid string, peer peer.ID, host host.Host) ([]byte, error) {
	// make a persistent stream for tx data requests
	txStream, err := host.NewStream(ctx, peer, ProtocolIDTransaction)
	if err != nil {
		return nil, err
	}
	defer txStream.Close()

	txStream.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = txStream.Write([]byte(getTxMsgPrefix + txid))
	if err != nil {
		return nil, err
	}

	// rd := bufio.NewReader(txStream)
	// var txid [64]byte
	resp, err := io.ReadAll(txStream)
	if err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, errors.New("stream closed without response")
	}
	if bytes.Equal(resp, []byte("0")) {
		return nil, errors.New("tx not found")
	}
	// rawTx, err := hex.DecodeString(string(resp))
	// if err != nil {
	// 	return nil, err
	// }
	return resp, nil
}
