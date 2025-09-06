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

func (rf *Raft) sendEntries(isHeartbeat bool) {
	if rf.state != leader {
		return
	}

	for peerIdx, _ := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		followerNextIdx := rf.nextIndex[peerIdx]

		if followerNextIdx > rf.getLastLogIndex() && !isHeartbeat { // follower is ahead, and it is not heartbeat, so don't send
			continue
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: followerNextIdx - 1,
			PrevLogTerm:  rf.logs[followerNextIdx-1].Term,
			LeaderCommit: rf.commitIndex,
		}

		entries := rf.logs[followerNextIdx:]
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)

		go rf.sendAppendEntry(peerIdx, args, &AppendEntriesReply{})
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// if server is no longer a leader or term has already changed, return
	if rf.state != leader || rf.currentTerm != args.Term || rf.currentTerm > reply.Term {
		return
	}

	// receiving server's term was higher, so step down to follower, update term and return
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.updateTerm(args.Term)
		return
	}

	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} // TODO
}
