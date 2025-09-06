package raft

import (
	"math/rand"
)

const (
	electionTimeoutMin = 250
	electionTimeoutMax = 400
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

	for peerIdx, _ := range rf.peers {
		if peerIdx != rf.me {
			go rf.sendRequestVote(peerIdx, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) setElectionTimer() {
	rf.electionTimeout = rand.Intn(electionTimeoutMax-electionTimeoutMin) + electionTimeoutMin
}
