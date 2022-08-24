package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"assignment4/src/labrpc"
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

const minDuration int = 500

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	logIndex int
	logTerm  int
	command  interface{}
}

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state           State
	applyCh         chan ApplyMsg
	electionTimer   *time.Timer
	electionTimeout time.Duration

	voteCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	currentTerm, voteFor := 0, 0
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		DPrintf("Error in unmarshall raft state")
	}
	rf.currentTerm, rf.votedFor, rf.log = currentTerm, voteFor, log
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("currentTerm: %d, args Term: %d, server %d received from %d", rf.currentTerm, args.Term, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
		}
		lastLogIndex := rf.log[len(rf.log)-1].logIndex
		lastLogTerm := rf.log[len(rf.log)-1].logTerm

		upToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if upToDate {
			rf.resetTimer()
		}

		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
			reply.VoteGranted = true
			rf.state = Follower
			rf.votedFor = args.CandidateId
			DPrintf("Server %d voted for Server %d\n", rf.me, args.CandidateId)
		} else {
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleAppendLog() {

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, isLeader
	}

	logEndId := rf.log[len(rf.log)-1].logIndex

	logEndtry := LogEntry{logIndex: logEndId + 1, logTerm: rf.currentTerm, command: command}

	rf.log = append(rf.log, logEndtry)

	go rf.handleAppendLog()
	rf.persist()
	return logEndId + 1, rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.applyCh = applyCh
	rf.electionTimeout = time.Millisecond * time.Duration(minDuration+rand.Intn(minDuration))
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetTimer()

	return rf
}

func (rf *Raft) resetTimer() {
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.electionTimer = time.AfterFunc(rf.electionTimeout, func() { rf.campaign() })
}

func (rf *Raft) campaign() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		rf.state = Candidate
		rf.voteCount = 1
		rf.currentTerm++
		rf.votedFor = rf.me

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log[len(rf.log)-1].logIndex,
			LastLogTerm:  rf.log[len(rf.log)-1].logTerm,
		}

		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			go func(s int, a RequestVoteArgs) {
				var reply RequestVoteReply
				DPrintf("Server %d send request vote to Server %d\n", rf.me, s)
				ok := rf.sendRequestVote(s, a, &reply)
				if ok {
					rf.handleRequestVote(&reply)
				}
			}(server, args)
		}
	}

	rf.resetTimer()
}

func (rf *Raft) handleRequestVote(r *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if r.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = r.Term
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if rf.state == Candidate && r.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.state = Leader

			rf.resetTimer()
		}
	}
}
