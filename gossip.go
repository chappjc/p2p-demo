package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

const (
	TopicTxs = "txs"
)

func startTxGossip(ctx context.Context, host host.Host) error {
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
				return
			}

			// if txMsg.Local {
			// 	continue
			// }

			fmt.Printf("received tx msg from %x (rcvd from %s), data = %x\n",
				txMsg.From, txMsg.ReceivedFrom, txMsg.Data)

			// txMsg.ID
			// txMsg.ReceivedFrom
			// txMsg.ValidatorData
			// txMsg.Local

			// fmt.Printf("msg.Data: %x\n", txMsg.Message.Data)
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

/*
	if err := topic.Publish(ctx, data); err != nil {
		fmt.Println("Failed to publish peer info:", err)
	}
	time.Sleep(30 * time.Second) // Publish every 30 seconds
*/

/*
go func() {
	for {
		msg, err := subscription.Next(ctx)
		if err != nil {
			fmt.Println("Failed to get message:", err)
			return
		}
	}
}()
*/

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
