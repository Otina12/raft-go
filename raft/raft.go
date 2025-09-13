package raft

// Paper: https://raft.github.io/raft.pdf

// This is an outline of the API that raft must expose to the service (or tester).
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   Start agreement on a new log entry.
// rf.GetState() (term, isLeader)
//   Ask a Raft for its current term, and whether it thinks it is leader.
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer should send an ApplyMsg to the service (or tester) in the same server.

import (
	"Raft/mygob"
	"Raft/myrpc"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

// ApplyMsg struct -
// As each Raft peer becomes aware that successive log entries are committed, the peer should send an ApplyMsg to the service (or tester) on the same server,
// via the applyCh passed to Make().
// Set CommandValid to true to indicate that the ApplyMsg contains a newly committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type ServerState string

const (
	leader    ServerState = "leader"
	candidate ServerState = "candidate"
	follower  ServerState = "follower"
)

const (
	heartbeatTimeout = 50
)

// Raft struct -
// Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*myrpc.ClientEnd // RPC end points of all peers
	persister *Persister         // Object to hold this peer's persisted state
	me        int                // this peer's index into peers[]
	dead      int32              // set by Kill()

	// Server state
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Extra state
	state           ServerState
	electionTimeout int
	voteCount       int
	applyCh         chan ApplyMsg
	heartbeatCh     chan bool
	grantVoteCh     chan bool
	winElectionCh   chan bool
	stepDownCh      chan bool
}

// GetState method -
// returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == leader
}

// Save Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
// See paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := mygob.NewEncoder(writer)

	// persist non-volatile state
	if encoder.Encode(rf.currentTerm) != nil || encoder.Encode(rf.votedFor) != nil || encoder.Encode(rf.logs) != nil {
		panic("raft: failed to persist server state")
	}

	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := mygob.NewDecoder(reader)

	if decoder.Decode(&rf.currentTerm) != nil || decoder.Decode(&rf.votedFor) != nil || decoder.Decode(&rf.logs) != nil {
		panic("raft: failed to read server state")
	}
}

// CondInstallSnapshot method -
// A service wants to switch to snapshot.
// Only do so if Raft hasn't had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot method -
// The service says it has created a snapshot that has all info up to and including index.
// This means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs struct -
// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply struct -
// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	// candidate's term is lower than ours, reply false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// candidate's term is higher than ours, so step down to follower and update term
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.updateTerm(args.Term)
	}

	reply.Term = args.Term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.sendToChannel(rf.grantVoteCh, true)
	}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.sendToChannel(rf.heartbeatCh, true)

	reply = &AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: false,
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.updateTerm(args.Term)
	}

	if rf.state == candidate {
		rf.state = follower
	}

	if rf.currentTerm > args.Term {
		return
	}

	// leader's log is longer than follower's log
	if args.PrevLogIndex > rf.getLastLogIndex() {
		return
	}

	// existing entry conflicts with new one (same index but different terms)
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		return
	}

	// at this point, we have found the matching index

	// many retries (log index decrements) could have happened, to the point where args.PrevLogIndex < rf.getLastLogIndex(),
	// so we first truncate follower's log, and only then append entries
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		go rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	for i := rf.lastApplied; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

// Start method -
// The service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log.
// If this server isn't the leader, returns false.
// Otherwise, start the agreement and return immediately.
// There is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election.
// Even if the Raft instance has been killed, this function should return gracefully.
//
// The first return value is the index that the command will appear at if it's ever committed.
// The second return value is the current term.
// The third return value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill method -
// The tester doesn't halt goroutines created by Raft after each test, but it does call the Kill() method.
// Code can use killed() to check whether Kill() has been called.
// The use of atomic avoids the need for a lock.
//
// The issue is that long-running goroutines use memory and may chew up CPU time, perhaps causing later tests to fail and generating confusing debug output.
// Any goroutine with a long-running loop should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The run goroutine manages the server. It is the main method which is always running and waiting for events.
func (rf *Raft) run() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
				rf.becomeCandidate(follower)
			}
		case candidate:
			select {
			case <-rf.stepDownCh:
				// already follower
			case <-rf.winElectionCh:
				rf.becomeLeader() // TODO
			case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond): // restart election
				rf.becomeCandidate(candidate)
			}
		case leader:
			select {
			case <-rf.stepDownCh:
				// already follower
			case <-time.After(heartbeatTimeout * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		}
	}
}

// Make creates the Raft server.
// The ports of all the Raft servers (including this one) are in peers[].
// This server's port is peers[me].
// All the servers' peers[] arrays have the same order.
// Persister is a place for this server to  save its persistent state, and also initially holds the most recent saved state, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*myrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1 // no vote yet
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = follower
	rf.setElectionTimer()
	rf.voteCount = 0
	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.winElectionCh = make(chan bool)
	rf.stepDownCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start run goroutine to start elections
	go rf.run()

	return rf
}

func (rf *Raft) isCandidateUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool {
	myLastLogIndex := rf.getLastLogIndex()
	myLastLogTerm := rf.getLastLogTerm()

	if candidateLastLogTerm < myLastLogTerm {
		return false
	} else if candidateLastLogTerm > myLastLogTerm {
		return true
	}

	return candidateLastLogIndex >= myLastLogIndex
}

func (rf *Raft) becomeCandidate(fromState ServerState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.state = candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate {
		return
	}

	rf.state = leader
	rf.broadcastAppendEntries()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}
