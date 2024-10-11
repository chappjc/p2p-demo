package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

var (
	discoverPeersMsg = "discover_peers"
)

type peerMan struct {
	h host.Host
	// nodeType string // e.g., "leader", "validator", "sentry"
}

var _ discovery.Discoverer = (*peerMan)(nil) // FindPeers method

func (pm *peerMan) discoveryStreamHandler(s network.Stream) {
	defer s.Close()

	sc := bufio.NewScanner(s)
	for sc.Scan() {
		msg := sc.Text()
		if msg != discoverPeersMsg {
			continue
		}

		peers := getKnownPeers(pm.h)
		// filteredPeers := filterPeersForNodeType(peers, nodeType)

		if err := sendPeersToStream(s, peers); err != nil {
			fmt.Println("failed to send peer list to peer", err)
			return
		}
	}

	fmt.Println("done sending peers on stream", s.ID())
}

// func (pm *peerMan) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
// 	return 0, nil
// }

func (pm *peerMan) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	peerChan := make(chan peer.AddrInfo)

	go func() {
		defer close(peerChan)
		for _, peerID := range pm.h.Network().Peers() {
			if peerID == pm.h.ID() {
				continue // Skip self
			}

			streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			stream, err := pm.h.NewStream(streamCtx, peerID, ProtocolIDDiscover)
			if err != nil {
				fmt.Println("Failed to open stream:", err)
				continue
			}
			defer stream.Close()

			stream.SetDeadline(time.Now().Add(5 * time.Second))

			requestMessage := discoverPeersMsg
			if _, err := stream.Write([]byte(requestMessage + "\n")); err != nil {
				fmt.Println("Failed to send request:", err)
				continue
			}

			var peers []peer.AddrInfo
			if err := readPeersFromStream(stream, &peers); err != nil {
				fmt.Println("Failed to read peers from stream:", err)
				continue
			}

			for _, p := range peers {
				peerChan <- p
			}
		}
	}()

	return peerChan, nil
}

type AddrInfo struct {
	ID    peer.ID               `json:"id"`
	Addrs []multiaddr.Multiaddr `json:"addrs"`
}

type PeerInfo struct {
	AddrInfo
	Protos []protocol.ID `json:"protos"`
}

func getKnownPeers(h host.Host) []PeerInfo {
	var peers []PeerInfo
	for _, peerID := range h.Network().Peers() { // connected peers only
		addrs := h.Peerstore().Addrs(peerID)

		supportedProtos, err := h.Peerstore().GetProtocols(peerID)
		if err != nil {
			fmt.Printf("GetProtocols for %v: %v\n", peerID, err)
			continue
		}

		peers = append(peers, PeerInfo{
			AddrInfo: AddrInfo{
				ID:    peerID,
				Addrs: addrs,
			},
			Protos: supportedProtos,
		})

	}
	return peers
}

func sendPeersToStream(s network.Stream, peers []PeerInfo) error {
	encoder := json.NewEncoder(s)
	if err := encoder.Encode(peers); err != nil {
		return fmt.Errorf("failed to encode peers: %w", err)
	}
	return nil
}

// addPeerToPeerStore adds a discovered peer to the local peer store.
func addPeerToPeerStore(h host.Host, p peer.AddrInfo) {
	// Only add the peer if it's not already in the peer store.
	// h.Peerstore().Peers()
	addrs := h.Peerstore().Addrs(p.ID)
	for _, addr := range p.Addrs {
		if !multiaddr.Contains(addrs, addr) {
			h.Peerstore().AddAddr(p.ID, addr, time.Hour)
			fmt.Println("Added new peer address to store:", p.ID, addr)
		}
	}

	// Add the peer's addresses to the peer store.
	// for _, addr := range p.Addrs {
	// 	h.Peerstore().AddAddr(p.ID, addr, time.Hour)
	// 	fmt.Println("Added new peer to store:", p.ID, addr)
	// }

	// and connect?
}

// readPeersFromStream reads a list of peer addresses from the stream.
func readPeersFromStream(s network.Stream, peers *[]peer.AddrInfo) error {
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(peers); err != nil {
		return fmt.Errorf("failed to decode peers: %w", err)
	}
	return nil
}

/*
type PeerMetadata struct {
	Info peer.AddrInfo
	Type string // e.g., "leader", "validator", "sentry"
}

// filterPeersForNodeType filters peers based on the requesting node's type.
func filterPeersForNodeType(peers []PeerMetadata, requesterType string) []PeerMetadata {
	var filtered []PeerMetadata

	for _, p := range peers {
		// share "leader" peers only with "validators"
		if p.Type == "leader" && requesterType != "validator" {
			continue // don't share "leader" peer info with non-validator nodes
		}

		// Add other policies as needed, e.g., only sharing "sentry" nodes with "leader" nodes

		// If the peer passes the filtering conditions, add it to the list
		filtered = append(filtered, p)
	}

	return filtered
}
*/
