package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
)

type Node struct {
	pm  *peerMan
	txi *transactionIndex
	// bki *blockIndex

	host host.Host
	wg   sync.WaitGroup
}

func NewNode(port uint64, privKey []byte) (*Node, error) {
	host, err := newHost(port, privKey)
	if err != nil {
		return nil, err
	}

	txi := newTransactionIndex()
	host.SetStreamHandler(ProtocolIDTransaction, txi.txStreamHandler)

	pm := &peerMan{h: host}
	host.SetStreamHandler(ProtocolIDDiscover, pm.discoveryStreamHandler)

	return &Node{
		host: host,
		pm:   pm,
		txi:  txi,
	}, nil
}

func (n *Node) Addr() string {
	hosts, ports := hostPort(n.host)
	id := n.host.ID()
	if len(hosts) == 0 {
		return ""
	}
	return fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s\n", hosts[0], ports[0], id)
}

func (n *Node) ID() string {
	return n.host.ID().String()
}

func (n *Node) checkPeerProtos(ctx context.Context, peer peer.ID) error {
	for _, pid := range []protocol.ID{
		ProtocolIDDiscover,
		ProtocolIDTransaction,
		pubsub.GossipSubID_v12,
	} {
		ok, err := checkProtocolSupport(ctx, n.host, peer, pid)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("protocol not supported: %v", pid)
		}
	}
	return nil
}

func (n *Node) Start(ctx context.Context, peers ...string) error {
	if err := startTxGossip(ctx, n.host, n.txi); err != nil {
		return err
	}

	// connect to bootstrap peers, if any
	for _, peer := range peers {
		peerInfo, err := connectPeer(ctx, peer, n.host)
		if err != nil {
			fmt.Printf("failed to connect to %v: %v", peer, err)
			continue
		}
		fmt.Println("connected to ", peerInfo)
		if err = n.checkPeerProtos(ctx, peerInfo.ID); err != nil {
			fmt.Printf("WARNING: peer does not support required protocols %v: %v", peer, err)
			//n.host.Network().ConnsToPeer(peerInfo.ID)
			// n.host.Peerstore().RemovePeer()
			// n.host.Network().InterfaceListenAddresses() // expands 0.0.0.0
			if err = n.host.Network().ClosePeer(peerInfo.ID); err != nil {
				fmt.Printf("failed to disconnect from %v: %v", peer, err)
			}
			continue
		}
		// n.host.ConnManager().TagPeer(peerID, "validatorish", 1)
	} // else would use persistent peer store (address book)

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			// discover for this node
			peerChan, err := n.pm.FindPeers(ctx, "kwil_namespace")
			if err != nil {
				fmt.Println("FindPeers:", err)
			} else {
				// Listen for discovered peers
				go func() {
					for peer := range peerChan {
						addPeerToPeerStore(n.host, peer)
					}
				}()
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Second):
			}
		}
	}()

	n.wg.Wait()

	return n.host.Close()
}

func newKey(r io.Reader) crypto.PrivKey {
	privKey, _, _ := crypto.GenerateECDSAKeyPair(r)
	return privKey
}

func loadKey(raw []byte) (crypto.PrivKey, error) {
	return crypto.UnmarshalECDSAPrivateKey(raw)
}

func newHost(port uint64, privKey []byte) (host.Host, error) {
	privKeyP2P, err := crypto.UnmarshalECDSAPrivateKey(privKey)
	if err != nil {
		return nil, err
	}
	sourceMultiAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))

	// listenAddrs := libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/tcp/0/ws")
	// security := libp2p.Security(libp2ptls.ID, libp2ptls.New)

	// TODO: use persistent peerstore

	return libp2p.New(
		libp2p.Transport(tcp.NewTCPTransport),
		// security, // default is noise
		libp2p.ListenAddrs(sourceMultiAddr),
		// listenAddrs,
		libp2p.Identity(privKeyP2P),
	) // libp2p.RandomIdentity, in-mem peer store, ...
}

func hostPort(host host.Host) ([]string, []int) {
	var addrStr []string
	var ports []int
	for _, addr := range host.Addrs() { // host.Network().ListenAddresses()
		ps, _ := addr.ValueForProtocol(multiaddr.P_TCP)
		port, _ := strconv.Atoi(ps)
		ports = append(ports, port)
		as, _ := addr.ValueForProtocol(multiaddr.P_IP4)
		addrStr = append(addrStr, as)

		fmt.Println("Listening on", addr)
	}

	return addrStr, ports
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

func checkProtocolSupport(_ context.Context, h host.Host, peerID peer.ID, protoID protocol.ID) (bool, error) {
	// Check if the peer supports the specified protocol
	supportedProtos, err := h.Peerstore().GetProtocols(peerID)
	if err != nil {
		return false, err
	}
	log.Printf("protos supported by %v: %v\n", peerID, supportedProtos)

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
			log.Println("Failed to check protocols for peer:", peerID, err)
			continue
		}

		if supportsProto {
			log.Printf("Peer %s supports protocol %s\n", peerID, pid)
		} else {
			log.Printf("Peer %s does NOT support protocol %s\n", peerID, pid)
		}
	}
}
