package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ProtocolIDDiscover    protocol.ID = "/kwil/discovery/1.0.0"
	ProtocolIDTransaction protocol.ID = "/kwil/transaction/1.0.0"
	ProtocolIDBlock       protocol.ID = "/kwil/block/1.0.0"

	getTxMsgPrefix  = "gettx:"
	getBlkMsgPrefix = "getblk:"

	testTxid = "1c577d897bb6cef3cffb8a6e323289eec5c85024dacd188d485fbf3bb003bb76"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	if err := run(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}

var (
	key       string
	port      uint64
	connectTo string
)

func run(ctx context.Context) error {
	flag.StringVar(&key, "key", "", "private key bytes (hexadecimal), empty is pseudo-random")
	flag.Uint64Var(&port, "port", 0, "listen port (0 for random)")
	flag.StringVar(&connectTo, "connect", "", "peer multiaddr to connect to")
	flag.Parse()

	rr := rand.Reader
	if port != 0 { // deterministic key based on port for testing
		rr = mrand.New(mrand.NewSource(int64(port)))
	}

	var rawKey []byte
	if key == "" {
		privKey := newKey(rr)
		rawKey, _ = privKey.Raw()
		log.Printf("priv key: %x\n", rawKey)
	} else {
		var err error
		rawKey, err = hex.DecodeString(key)
		if err != nil {
			return err
		}
	}

	node, err := NewNode(port, rawKey)
	if err != nil {
		return err
	}

	addr := node.Addr()
	log.Printf("to connect: %s\n", addr)

	var bootPeers []string
	if connectTo != "" {
		bootPeers = append(bootPeers, connectTo)
	}
	if err = node.Start(ctx, bootPeers...); err != nil {
		return err
	}
	// Start is blocking, for now.

	return nil
}
