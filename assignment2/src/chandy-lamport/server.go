package chandy_lamport

import "log"

type Snapshot struct {
	id       int
	tokens   int // key = server ID, value = num tokens
	messages []*SnapshotMessage
}

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
	snapshotState *SyncMap                //map[int]*Snapshot
	isRecording   map[int]map[string]bool // key = src
	completed     map[int]map[string]bool // key = src
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
		NewSyncMap(),
		make(map[int]map[string]bool),
		make(map[int]map[string]bool),
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

type Key struct {
	snapshotId int
	src        string
}

type Record struct {
	isRecording bool
	tokens      int
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME

	switch message := message.(type) {
	case TokenMessage:
		server.Tokens += message.numTokens

		server.snapshotState.Range(func(k, v interface{}) bool {
			if snapshotId, isInt := k.(int); isInt {
				if isRecording, found := server.isRecording[snapshotId][src]; found && isRecording {
					if snapshot, ok := v.(*Snapshot); ok {
						snapshot.messages = append(snapshot.messages, &SnapshotMessage{src, server.Id, message})
					}
				}
			}

			return true
		})

	case MarkerMessage:
		if _, found := server.snapshotState.LoadOrStore(message.snapshotId, &Snapshot{message.snapshotId, server.Tokens, make([]*SnapshotMessage, 0)}); !found {
			server.SendToNeighbors(message)
			server.completed[message.snapshotId] = make(map[string]bool)
			server.isRecording[message.snapshotId] = make(map[string]bool)
			for _, source := range getSortedKeys(server.inboundLinks) {
				if src != source {
					server.isRecording[message.snapshotId][source] = true
				} else {
					server.isRecording[message.snapshotId][source] = false
				}
			}
		} else {
			server.isRecording[message.snapshotId][src] = false
		}

		server.completed[message.snapshotId][src] = true

		if len(server.completed[message.snapshotId]) == len(server.inboundLinks) {
			server.sim.NotifySnapshotComplete(server.Id, message.snapshotId)
		}

	default:
		log.Fatal("Error unknown message: ", message)
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	server.snapshotState.LoadOrStore(snapshotId, &Snapshot{snapshotId, server.Tokens, make([]*SnapshotMessage, 0)})
	server.isRecording[snapshotId] = make(map[string]bool)
	server.completed[snapshotId] = make(map[string]bool)
	for _, src := range getSortedKeys(server.inboundLinks) {
		server.isRecording[snapshotId][src] = true
	}
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
