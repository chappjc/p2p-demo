package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

const (
	TopicTxs = "txs"
)

func startTxGossip(ctx context.Context, host host.Host, txi *transactionIndex) error {
	_, topicTx, subTx, err := subTxs(ctx, host)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}

			txid := randBytes(32)
			txi.storeTx(hex.EncodeToString(txid), randBytes(10))
			fmt.Printf("announcing txid %x\n", txid)
			err := topicTx.Publish(ctx, txid)
			if err != nil {
				fmt.Println("Publish:", err)
				return
			}
		}
	}()

	me := host.ID()

	go func() {
		for {
			txMsg, err := subTx.Next(ctx)
			if err != nil {
				fmt.Println("subTx.Next:", err)
				return
			}

			if string(txMsg.From) == string(me) {
				fmt.Println("message from me ignored")
				continue
			}

			txid := hex.EncodeToString(txMsg.Data)

			have := txi.have(txid)
			fmt.Printf("received tx msg from %v (rcvd from %s), data = %x, already have = %v\n",
				txMsg.GetFrom(), txMsg.ReceivedFrom, txMsg.Message.Data, have)
			if have {
				continue
			}

			// Now we use getTx with the ProtocolIDTransaction stream
			fmt.Println("fetching tx", txid)
			txRaw, err := getTx(ctx, txid, txMsg.GetFrom(), host)
			if err != nil {
				fmt.Println("getTx:", err)
				continue
			}
			txi.storeTx(txid, txRaw)

			// txMsg.ID
			// txMsg.ReceivedFrom
			// txMsg.ValidatorData
			// txMsg.Message.Signature
		}
	}()

	return nil
}

func subTxs(ctx context.Context, host host.Host) (*pubsub.PubSub, *pubsub.Topic, *pubsub.Subscription, error) {
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		return nil, nil, nil, err
	}

	// Join the discovery topic
	topic, err := ps.Join(TopicTxs)
	if err != nil {
		return nil, nil, nil, err
	}

	// Subscribe to the discovery topic
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, nil, nil, err
	}
	return ps, topic, sub, nil
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
