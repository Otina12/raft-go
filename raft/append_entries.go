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

func (rf *Raft) sendEntries() {
	if rf.state != leader {
		return
	}

	// TODO: fill in args
	args := &AppendEntriesArgs{}

	for peerIdx, _ := range rf.peers {
		if peerIdx != rf.me {
			go rf.sendAppendEntry(peerIdx, args, &AppendEntriesReply{})
		}
	}
}
