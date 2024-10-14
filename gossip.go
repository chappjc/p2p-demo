package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	TopicTxs  = "txs"
	TopicBlks = "blks"
)

func (n *Node) startTxGossip(ctx context.Context, ps *pubsub.PubSub) error {
	topicTx, subTx, err := subTxs(ctx, ps)
	if err != nil {
		return err
	}

	subCanceled := make(chan struct{})

	n.wg.Add(1)
	go func() {
		defer func() {
			<-subCanceled
			topicTx.Close()
			n.wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}

			txid := randBytes(32)
			n.txi.storeTx(hex.EncodeToString(txid), randBytes(10))
			fmt.Printf("announcing txid %x\n", txid)
			err := topicTx.Publish(ctx, txid)
			if err != nil {
				fmt.Println("Publish:", err)
				return
			}
		}
	}()

	me := n.host.ID()

	go func() {
		defer close(subCanceled)
		defer subTx.Cancel()
		for {
			txMsg, err := subTx.Next(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					fmt.Println("subTx.Next:", err)
				}
				return
			}

			if string(txMsg.From) == string(me) {
				fmt.Println("message from me ignored")
				continue
			}

			txid := hex.EncodeToString(txMsg.Data)

			have := n.txi.have(txid)
			fmt.Printf("received tx msg from %v (rcvd from %s), data = %x, already have = %v\n",
				txMsg.GetFrom(), txMsg.ReceivedFrom, txMsg.Message.Data, have)
			if have {
				continue
			}

			// Now we use getTx with the ProtocolIDTransaction stream
			fmt.Println("fetching tx", txid)
			txRaw, err := getTx(ctx, txid, txMsg.GetFrom(), n.host)
			if err != nil {
				fmt.Println("getTx:", err)
				continue
			}
			n.txi.storeTx(txid, txRaw)

			// txMsg.ID
			// txMsg.ReceivedFrom
			// txMsg.ValidatorData
			// txMsg.Message.Signature
		}
	}()

	return nil
}

func subTxs(ctx context.Context, ps *pubsub.PubSub) (*pubsub.Topic, *pubsub.Subscription, error) {
	return subTopic(ctx, ps, TopicTxs)
}

func subBlks(ctx context.Context, ps *pubsub.PubSub) (*pubsub.Topic, *pubsub.Subscription, error) {
	return subTopic(ctx, ps, TopicBlks)
}

func subTopic(_ context.Context, ps *pubsub.PubSub, topic string) (*pubsub.Topic, *pubsub.Subscription, error) {
	// Join the discovery topic
	th, err := ps.Join(topic)
	if err != nil {
		return nil, nil, err
	}

	// Subscribe to the discovery topic
	sub, err := th.Subscribe()
	if err != nil {
		return nil, nil, err
	}
	return th, sub, nil
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
