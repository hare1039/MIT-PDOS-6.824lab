package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
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

func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%s send final result: {%d %d}", rf, rf.lastApplied, rf.commitIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("%s send final result: {%d %v}", rf, rf.lastApplied, rf.logs[rf.lastApplied].Command)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return // leader expired
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.resetTerm(args.Term, NullPeer)
	}

	rf.heartbeat <- args.LeaderID

	DPrintf("%s got heartbeat with log: %v", rf, args.Entries)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.lastIndex() {
		return
	}

	// 3. if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all thatfollow it (§5.3)
	leaderLogFirst := rf.logs[:args.PrevLogIndex+1]
	leaderLogSecond := rf.logs[args.PrevLogIndex+1:]
	conflict := false
	for i := 0; i < len(leaderLogSecond) && i < len(args.Entries); i++ {
		if leaderLogSecond[i].Term != args.Entries[i].Term {
			conflict = true
			break
		}
	}

	// 4. append any new entries not already in the log
	if len(args.Entries) > len(leaderLogSecond) {
		conflict = true
	}

	if conflict {
		rf.logs = append(leaderLogFirst, args.Entries...)
	}

	reply.Success = true
	reply.ConflictIndex = args.PrevLogIndex
	// 5. If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastIndex()
		}
		go rf.Commit()
	}
	//	rf.resetTerm(args.Term, args.LeaderID)
	DPrintf("%s updated Logs: %v", rf, rf.logs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s SendLogs: %v", rf, rf.logs)

	if rf.currentRole == RoleLeader {
		for i := range rf.peers {
			if i != rf.me {
				go func(peer int) {
					index := rf.nextIndex[peer]
					term := 0
					if index != 0 {
						term = rf.logs[index-1].Term
					}

					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: index - 1,
						PrevLogTerm:  term,
						Entries:      rf.logs[index:],
					}

					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(peer, &args, &reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !ok {
						return
					}

					if rf.currentRole != RoleLeader || rf.currentTerm != args.Term {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.resetTerm(reply.Term, NullPeer)
						return
					}

					// If last log index ≥ nextIndex for a follower: sendAppendEntries RPC with log entries starting at nextIndex
					// If successful: update nextIndex and matchIndex forfollower (§5.3)
					// If AppendEntries fails because of log inconsistency:decrement nextIndex and retry (§5.3)
					// ok
					if reply.Success {
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					} else {
						// failed, mark to unconflicted index
						rf.nextIndex[peer] = reply.ConflictIndex
					}

					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:set commitIndex = N (§5.3, §5.4).
					N := rf.lastIndex()
					count := 0
					for N > rf.commitIndex {
						for p := range rf.peers {
							if rf.matchIndex[p] >= N {
								count++
							}

							if count > len(rf.peers)/2 && rf.logs[N].Term == rf.currentTerm {
								rf.commitIndex = N
								go rf.Commit()
								break
							}
						}

					}
				}(i)
			}
		}
	}
}
