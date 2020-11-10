package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) send() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole == RoleLeader {
		for i := range rf.peers {
			if i != rf.me {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0, //rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
			}
		}

		//			if args.PrevLogIndex >= 0 {
		//				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		//			}
		//			if rf.nextIndex[i] <= rf.getLastIndex() {
		//				args.Entries = rf.logs[rf.nextIndex[i]:]
		//			}
		//
		//
	}
}
