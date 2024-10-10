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
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
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
	rawKey, _ = hex.DecodeString("307702010104200c69e64c05ecec17a4c640294d231a191b4eeb4f55fa0fb9e4d293ac010b0c10a00a06082a8648ce3d030107a14403420004aa9f1dc0e9fec8a1651f7d16b4d1ea36b6e3cdabfa742924fb3dadcc2fd9acfebd9cf8cf87159447470d81f15f84ba391c08c088d1a00ef9dc5acadc77c5ee76")

	port      uint64
	connectTo string
)

func run(ctx context.Context) error {
	flag.Uint64Var(&port, "port", 0, "listen port (0 for random)")
	flag.StringVar(&connectTo, "connect", "", "peer multiaddr to connect to")
	flag.Parse()

	rr := rand.Reader
	if port != 0 { // deterministic key based on port for testing
		rr = mrand.New(mrand.NewSource(int64(port)))
	}

	privKey, _, _ := crypto.GenerateECDSAKeyPair(rr)
	ecdsaPrivKey := privKey.(*crypto.ECDSAPrivateKey)
	rawKey, _ := crypto.MarshalECDSAPrivateKey(*ecdsaPrivKey)
	fmt.Printf("key: %x\n", rawKey)

	// ecdsaPrivKey, err := crypto.UnmarshalECDSAPrivateKey(rawKey)
	// if err != nil {
	// 	return err
	// }
	// var privKey crypto.PrivKey = ecdsaPrivKey

	sourceMultiAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))

	// listenAddrs := libp2p.ListenAddrStrings(
	// 	"/ip4/0.0.0.0/tcp/0",
	// 	// "/ip4/0.0.0.0/tcp/0/ws",
	// )
	// security := libp2p.Security(libp2ptls.ID, libp2ptls.New)

	host, err := libp2p.New(
		libp2p.Transport(tcp.NewTCPTransport),
		// security,
		libp2p.ListenAddrs(sourceMultiAddr),
		// listenAddrs,
		libp2p.Identity(privKey),
	) // libp2p.RandomIdentity, in-mem peer store, ...
	if err != nil {
		return err
	}

	fmt.Println("Host created, ID:", host.ID())
	var portStr string
	for _, addr := range host.Addrs() {
		ps, _ := addr.ValueForProtocol(multiaddr.P_TCP)
		if ps != "" {
			portStr = ps
		}
		fmt.Println("Listening on", addr)
	}
	log.Printf("to connect: /ip4/127.0.0.1/tcp/%v/p2p/%s\n", portStr, host.ID())

	/*for _, la := range host.Network().ListenAddresses() {
		if p, err := la.ValueForProtocol(multiaddr.P_TCP); err == nil {
			fmt.Println("port:", p)
			break
		}
	}*/

	// transaction data request stream and handler
	txi := newTransactionIndex()
	txi.txids[testTxid] = []byte{1, 2, 3}
	host.SetStreamHandler(ProtocolIDTransaction, txi.txStreamHandler)
	// host.SetStreamHandler(ProtocolIDBlock, txi.blockStreamHandler)

	fmt.Println(host.Peerstore().Peers())

	// connect to bootstrap peer, if any
	if connectTo != "" {
		peerInfo, err := connectPeer(ctx, connectTo, host)
		if err != nil {
			return err
		}

		rawTx, err := getTx(ctx, testTxid, peerInfo.ID, host)
		if err != nil {
			fmt.Println("getTx:", err)
		} else {
			fmt.Printf("rawTx: %x\n", rawTx)
		}
	} // else would use persistent peer store (address book)

	fmt.Println(host.Peerstore().Peers())

	// peer discovery protocol stream handler
	pm := &peerMan{h: host}
	host.SetStreamHandler(ProtocolIDDiscover, pm.discoveryStreamHandler)

	// backoff.NewBackoffDiscovery(pm, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			fmt.Println(host.Peerstore().Peers())
			// discover for this node
			peerChan, err := pm.FindPeers(ctx, "kwil_namespace")
			if err != nil {
				fmt.Println("FindPeers:", err)
			} else {
				// Listen for discovered peers
				go func() {
					for peer := range peerChan {
						addPeerToPeerStore(host, peer)
					}
					// fmt.Println("done with FindPeers")
				}()
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Second):
			}
		}
	}()

	wg.Wait()

	fmt.Println("shutting down...")

	return host.Close()
}

func checkProtocolSupport(ctx context.Context, h host.Host, peerID peer.ID, protoID protocol.ID) (bool, error) {
	// Check if the peer supports the specified protocol
	supportedProtos, err := h.Peerstore().GetProtocols(peerID)
	if err != nil {
		return false, err
	}
	fmt.Printf("protos supported by %v: %v\n", peerID, supportedProtos)

	// supportsProto, err := h.Peerstore().SupportsProtocols(peerID, pid)
	// if err != nil {
	// 	fmt.Println("Failed to check protocols for peer:", peerID, err)
	// 	continue
	// }

	// If the peer supports the protocol, print out the peer ID
	return slices.Contains(supportedProtos, protoID) /* len(supportsProto) > 0 */, nil
}

// checkProtocolSupportAll checks if the current connected peers support a given protocol.
func checkProtocolSupportAll(ctx context.Context, h host.Host, pid protocol.ID) {
	for _, peerID := range h.Network().Peers() {
		supportsProto, err := checkProtocolSupport(ctx, h, peerID, pid)
		if err != nil {
			fmt.Println("Failed to check protocols for peer:", peerID, err)
			continue
		}

		if supportsProto {
			fmt.Printf("Peer %s supports protocol %s\n", peerID, pid)
		} else {
			fmt.Printf("Peer %s does NOT support protocol %s\n", peerID, pid)
		}
	}
}

func connectPeer(ctx context.Context, addr string, host host.Host) (*peer.AddrInfo, error) {
	// Turn the destination into a multiaddr.
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Extract the peer ID from the multiaddr.
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Add the destination's peer multiaddress in the peerstore.
	// This will be used during connection and stream creation by libp2p.
	// host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)

	return info, host.Connect(ctx, *info)
}
