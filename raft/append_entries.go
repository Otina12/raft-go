package raft

// AppendEntriesArgs struct -
// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply struct -
// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != leader {
		return
	}

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		go rf.broadcastAppendEntryToServer(peerIdx)
	}
}

func (rf *Raft) broadcastAppendEntryToServer(server int) {
	rf.mu.Lock()

	if rf.state != leader {
		rf.mu.Unlock()
		return
	}

	followerNextIdx := rf.nextIndex[server]
	if followerNextIdx < 1 {
		followerNextIdx = 1
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: followerNextIdx - 1,
		PrevLogTerm:  rf.logs[followerNextIdx-1].Term,
		LeaderCommit: rf.commitIndex,
	}

	entries := make([]LogEntry, len(rf.logs[followerNextIdx:]))
	copy(entries, rf.logs[followerNextIdx:])
	args.Entries = entries

	rf.mu.Unlock()

	rf.sendAppendEntries(server, args, &AppendEntriesReply{})
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()

	doRetry := false

	// if server is no longer a leader or term has already changed, return
	if rf.state != leader || rf.currentTerm != args.Term || rf.currentTerm > reply.Term {
		rf.mu.Unlock()
		return
	}

	// receiving server's term was higher, so step down to follower, update term and return
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.updateTerm(reply.Term)
		rf.setElectionTimer()
		rf.sendToChannel(rf.stepDownCh, true)
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] -= 1
		}
		doRetry = true
	}

	// if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		if rf.logs[N].Term != rf.currentTerm {
			continue
		}

		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= N {
				count += 1
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLogs()
			break
		}
	}

	rf.persist()
	rf.mu.Unlock()

	if doRetry {
		rf.broadcastAppendEntryToServer(server)
	}
}
