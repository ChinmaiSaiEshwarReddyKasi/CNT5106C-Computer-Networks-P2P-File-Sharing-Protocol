# CNT5106C-Computer-Networks-P2P-File-Sharing-Protocol

This is a Peer-to-Peer File sharing application that works on a principle similar to that of BitTorrent consisting of Choking and Unchoking Mechanisms and the connections are made using the TCP protocol

In this project we have a group of independent computers that behave as a distributed repository of files, where each of them have some parts of the file that we intend to transfer

These computers, that act as peers, search and download the missing parts of the required file by requesting and connecting to the participating peer that has it, while also sharing the parts that they have.

## Protocol:

- In this project, we use TCP protocol to establish connection between peers wanting to share files with each other
- To share files, the peers first send a handshake message to each other, consisting of the header, zero bits and peer ID.
- Then a stream of data messages are sent which consists of message length, type and payload.
- There are various types of payloads like piece and bitfield. The types of messages are have, bitfield, choke, unchoke, interested, not interested, request and piece

## Working:

- The peers are started by startRemotePeers in the order that is specified in the PeerInfo config file, and the peerProcess takes the peer ID as a parameter.
- The peer that just started is supposed to make a TCP with every peer that is participating in the file sharing and has started before it.
- All the peers also read the common configuration file that contains the details of the file to be shared, its size, choking and unchoking intervals, and number of preferred neighbours.
- The PeerInfo file specifies whether a peer has the complete file by bits 0 or 1. Once a peer gets the complete file, the PeerInfo.cfg is updated with bit 1 for the corresponding peer.
- The first peer that is started, just listens on the port specified in the PeerInfo file as there are no other peers to connect to.
- Also, we maintain a log every peer for when they establish a TCP connection to another peer, when they change their preferred neighbours, when they change their optimistically unchoked neighbour, when they are choked or unchoked by another peer or when they receive have/interested/not interested messages and when they finish downloading a piece or the complete file.

## File Sharing:

If peer requires a file, it issues a search for the file using its filename, or some keyword along with a hop count of 1

- The request is issued in the overlay network to other peers that are at a distance of current hop count or less from the requesting peer, and the search request expires after a pre-decided number of hop count seconds. The duplicate search requests are not allowed.
- If a peer having the required file receives the search request, then it sends a response to the peer it received the request from. If the peer initiating the request receives the response, it consumes it, otherwise forwards it to the peer that it received the request from.
- If the requester gets the response, it collects all responses received until the expiry of the request, and those replies after it are just ignored.
The response from the peer that matches the required filename and piece index, is chosen, and the requesting peer then establishes a TCP connection with it. The requester peer then copies files from the sender peer, to its own directory and updates accordingly. Once it receives the file, the TCP connection is terminated.
- If the search request terminated without success, then the peer should re-initiate the search after increasing the hop count by 1. It should continue increasing the hop count until the search request succeeds, or the hop count exceeds a pre-defined hop count number.
Terminating the Protocol
- If the number of nodes exceeds the number of allowable hop counts, then their termination should be initiated. If a departing has just one neighbour, then it simply terminates the TCP connection with it else it chooses one of its neighbours as the neighbour of all its other neighbours (unless they aren't already neighbours). Then, it should terminate all its TCP connections and ongoing file transfers.
