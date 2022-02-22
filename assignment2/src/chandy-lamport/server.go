package chandy_lamport

import (
	"fmt"
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	snapLink   map[int]*SnapshotState
	snapState  []*SnapshotState
	l          sync.Mutex
	sendmarker bool
	r          map[string]bool // key = server.src to ready receive
	bs         map[string]bool // key = server.dst to block receive
	snc        chan *SendMessageEvent
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotState),
		[]*SnapshotState{},
		sync.Mutex{},
		false,
		make(map[string]bool),
		make(map[string]bool),
		make(chan *SendMessageEvent, 100),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	switch msg := message.(type) {
	case MarkerMessage:
		log.Printf("Server %v Received MarkedMessage from : %v\n", server.Id, src)
		_, ok := server.r[server.Id]
		if !ok {
			log.Printf("Server %v seen first MarkerMessage\n", server.Id)
			// empty state of inbound channel from src
			for _, link := range server.inboundLinks {
				if link.src == src {
					link.events.Empty()
				}
			}

			// snapshot firsttime after empty State of Channel
			server.sim.logger.RecordEvent(server, StartSnapshot{server.Id, msg.snapshotId})
			server.StartSnapshot(msg.snapshotId)
			// send marker to all neighboor
			// server.SendToNeighbors(MarkerMessage{snapshotId: msg.snapshotId})
			server.sendmarker = true
			// ready to received msg
			server.bs[src] = true
		} else {
			// already receiving marker message before
			fmt.Printf("Server %v already have snapshot before with %v snapState\n", server.Id, len(server.snapState))
			for _, value := range server.snapState[len(server.snapState)-1].messages {
				switch val := value.message.(type) {
				case TokenMessage:
					server.snapState[len(server.snapState)-1].tokens[server.Id] += val.numTokens
				default:
					break
				}
			}
			server.r[server.Id] = false
		}

	case TokenMessage:
		log.Printf("Server %v Received TokenMessage: %v\n", server.Id, msg)
		key, ok := server.r[server.Id]
		if ok && key {
			//server.snapState[len(server.snapState)-1].tokens[server.Id] = server.Tokens
			server.snapState[len(server.snapState)-1].messages = append(server.snapState[len(server.snapState)-1].messages, &SnapshotMessage{src, server.Id, &msg})
			//server.Tokens += msg.numTokens
		} else if !ok {
			server.Tokens += msg.numTokens
		}

	default:
		break
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	// append new snapshot
	_, ok := server.r[server.Id] // ok if exist map of server.Id
	// first time Snapshot
	if !ok {
		newSnapState := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
		newSnapState.id = snapshotId
		newSnapState.tokens[server.Id] = server.Tokens
		server.snapLink[snapshotId] = &newSnapState
		server.snapState = append(server.snapState, &newSnapState)
		server.sim.NotifySnapshotComplete(server.Id, snapshotId)
		server.SendToNeighbors(MarkerMessage{snapshotId})
		server.r[server.Id] = true
		fmt.Printf("Server %v have SnapState id: %v with token %d\n", server.Id, newSnapState.id, newSnapState.tokens[server.Id])
		fmt.Printf("Server %v have %d snapshot\n", server.Id, len(server.snapState))
	} else {
		//_, sok := server.snapLink[snapshotId] // ok if exist SnapshotState with snapshotId in srv
		//if sok {
		//st.tokens[server.Id] += server.Tokens
		//server.sim.NotifySnapshotComplete(server.Id, snapshotId)
		//server.SendToNeighbors(MarkerMessage{snapshotId})
		//}

	}
}
