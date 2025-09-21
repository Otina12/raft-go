package raft

import (
	"math/rand"
)

const (
	electionTimeoutMin = 350
	electionTimeoutMax = 500
)

func (rf *Raft) startElection() {
	if rf.state != candidate {
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for peerIdx := range rf.peers {
		if peerIdx != rf.me {
			go rf.sendRequestVote(peerIdx, args, &RequestVoteReply{})
		}
	}
}

// Example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// args is the RequestVoteArgs argument.
// reply is passed by caller and filled by the receiving server.

// The myrpc package simulates a lossy network, in which servers may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives within a timeout interval, Call() returns true;
// Otherwise Call() returns false.
// Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the handler function on the server side does not return.
// Thus, there is no need to implement your own timeouts around Call().
//
// Look at the comments in ../myrpc/rpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if server is no longer a candidate or term has already changed, return
	if rf.state != candidate || rf.currentTerm != args.Term || rf.currentTerm > reply.Term {
		return
	}

	// receiving server's term was higher, so step down to follower, update term and return
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.updateTerm(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCount += 1
		if rf.voteCount == len(rf.peers)/2+1 { // only send when exactly majority votes received
			rf.sendToChannel(rf.winElectionCh, true)
		}
	}
}

func (rf *Raft) setElectionTimer() {
	rf.electionTimeout = rand.Intn(electionTimeoutMax-electionTimeoutMin) + electionTimeoutMin
}
