package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	mrand2 "math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	noise "github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	//libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
)

const (
	dummyTxSize     = 40_000
	dummyTxInterval = 1 * time.Second
)

type Node struct {
	pm  *peerMan
	txi *transactionIndex
	bki *blockIndex

	host   host.Host
	pex    bool
	leader atomic.Bool
	wg     sync.WaitGroup
}

func NewNode(port uint64, privKey []byte, leader, pex bool) (*Node, error) {
	host, err := newHost(port, privKey)
	if err != nil {
		return nil, err
	}
	// n.host.Network().InterfaceListenAddresses() // expands 0.0.0.0

	txi := newTransactionIndex()
	host.SetStreamHandler(ProtocolIDGetTx, txi.txGetStreamHandler)

	pm := &peerMan{h: host}
	if pex {
		host.SetStreamHandler(ProtocolIDDiscover, pm.discoveryStreamHandler)
	} else {
		host.SetStreamHandler(ProtocolIDDiscover, func(s network.Stream) {
			s.Close()
		})
	}

	node := &Node{
		host: host,
		pm:   pm,
		pex:  pex,
		txi:  txi,
	}

	node.leader.Store(leader)

	host.SetStreamHandler(ProtocolIDTxAnn, node.txAnnStreamHandler)

	return node, nil
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
		ProtocolIDGetTx,
		ProtocolIDTxAnn,
		// pubsub.GossipSubID_v12,
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

// mine will simulate mining a block by harvesting transactions from the tx
// index (would be mempool), assembling a block, and advertising it to peers.
// Only the leader does this.
func (n *Node) mine(ctx context.Context) {
	N := 100
	for {
		if n.txi.size() < N || !n.leader.Load() {
			fmt.Println(n.txi.size(), n.leader.Load())
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		log.Println("MINED BLOCK -- serializing and announcing!")

		// Reap txns for the block
		blkTxs := n.txi.reapN(N)

		hasher := sha256.New()

		numTxBts := binary.LittleEndian.AppendUint64(nil, uint64(len(blkTxs)))
		hasher.Write(numTxBts)

		var buf bytes.Buffer // TODO: more efficient
		_, err := buf.Write(numTxBts)
		if err != nil {
			log.Println("failed to write block header")
			return
		}

		for txid, rawTx := range blkTxs {
			txhash, err := hex.DecodeString(txid)
			if err != nil {
				log.Println("invalid tx hash for block:", txid)
				return
			}
			buf.Write(txhash)
			hasher.Write(txhash)

			buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(rawTx))))
			buf.Write(rawTx)
		}

		blkHash := hasher.Sum(nil)
		blkID := hex.EncodeToString(blkHash)
		rawBlk := buf.Bytes()

		log.Printf("ANNOUNCING BLOCK %v size %d\n", blkID, len(rawBlk))

		go n.announceBlk(ctx, blkID, rawBlk, n.host.ID())
	}
}

// Start begins tx and block gossip, connects to any bootstrap peers, and begins
// peer discovery.
func (n *Node) Start(ctx context.Context, peers ...string) error {
	// ps, err := pubsub.NewGossipSub(ctx, n.host)
	// if err != nil {
	// 	return err
	// }
	// if err := n.startTxGossip(ctx, ps); err != nil { // gossip.go
	// 	return err
	// }

	// custom stream-based gossip uses txAnnStreamHandler and announceTx.
	// This dummy method will make create+announce new pretend transactions.
	n.startTxAnns(ctx, dummyTxInterval, dummyTxSize) // nogossip.go

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.mine(ctx)
	}()

	// connect to bootstrap peers, if any
	for _, peer := range peers {
		peerInfo, err := connectPeer(ctx, peer, n.host)
		if err != nil {
			log.Printf("failed to connect to %v: %v", peer, err)
			continue
		}
		log.Println("connected to ", peerInfo)
		if err = n.checkPeerProtos(ctx, peerInfo.ID); err != nil {
			log.Printf("WARNING: peer does not support required protocols %v: %v", peer, err)
			if err = n.host.Network().ClosePeer(peerInfo.ID); err != nil {
				log.Printf("failed to disconnect from %v: %v", peer, err)
			}
			// n.host.Peerstore().RemovePeer()
			continue
		}
		// n.host.ConnManager().TagPeer(peerID, "validatorish", 1)
	} // else would use persistent peer store (address book)

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if !n.pex {
			return
		}
		for {
			// discover for this node
			peerChan, err := n.pm.FindPeers(ctx, "kwil_namespace")
			if err != nil {
				log.Println("FindPeers:", err)
			} else {
				go func() {
					for peer := range peerChan {
						addPeerToPeerStore(n.host.Peerstore(), peer)
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

	log.Println("node tx/block gossip started, and peer discovery enabled")

	<-ctx.Done()

	n.wg.Wait()

	return n.host.Close()
}

type randSrc struct{}

func (randSrc) Uint64() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}

var rng = mrand2.New(randSrc{})

func (n *Node) peers() []peer.ID {
	peers := n.host.Network().Peers()
	rng.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
	return peers
}

func NewKey(r io.Reader) crypto.PrivKey {
	// priv, _ := secp256k1.GeneratePrivateKeyFromRand(r)
	// privECDSA := priv.ToECDSA()
	// privECDSA.Curve = elliptic.P256()
	// privKey, _, _ := crypto.ECDSAKeyPairFromKey(privECDSA)
	//privKey, _, err := crypto.GenerateECDSAKeyPair(r)
	pk, err := secp256k1.GeneratePrivateKeyFromRand(r)
	if err != nil {
		panic(err)
	}
	privKey := (*crypto.Secp256k1PrivateKey)(pk)
	// privKey, _, err := crypto.GenerateSecp256k1Key(r)
	return privKey
}

func newHost(port uint64, privKey []byte) (host.Host, error) {
	privKeyP2P, err := crypto.UnmarshalSecp256k1PrivateKey(privKey) // rypto.UnmarshalECDSAPrivateKey(privKey)
	if err != nil {
		return nil, err
	}
	sourceMultiAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))

	// listenAddrs := libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/tcp/0/ws")

	// TODO: use persistent peerstore

	return libp2p.New(
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
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

func checkProtocolSupport(_ context.Context, h host.Host, peerID peer.ID, protoIDs ...protocol.ID) (bool, error) {
	supported, err := h.Peerstore().SupportsProtocols(peerID, protoIDs...)
	if err != nil {
		return false, fmt.Errorf("Failed to check protocols for peer %v: %w", peerID, err)
	}
	return len(protoIDs) == len(supported), nil

	// supportedProtos, err := h.Peerstore().GetProtocols(peerID)
	// if err != nil {
	// 	return false, err
	// }
	// log.Printf("protos supported by %v: %v\n", peerID, supportedProtos)

	// for _, protoID := range protoIDs {
	// 	if !slices.Contains(supportedProtos, protoID) {
	// 		return false, nil
	// 	}
	// }
	// return true, nil
}
