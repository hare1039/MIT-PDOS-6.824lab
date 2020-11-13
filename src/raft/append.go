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

	for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logAt(index).Command,
			CommandIndex: index,
		}
		DPrintf("%s send final result: {%d %v}", rf, index, rf.logAt(index).Command)
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = rf.logLength()

	// 1. reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return // leader expired
	}

	if args.Term > rf.currentTerm {
		defer rf.persist()
		reply.Term = args.Term
		rf.resetTerm(args.Term, NullPeer)
	}

	rf.heartbeat <- args.LeaderID

	DPrintf("%s got heartbeat with log: %v", rf, args.Entries)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.lastIndex() {
		return
	}

	// check terms
	if args.PrevLogIndex > rf.lastIncludedIndex && rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		for i := rf.logBegin(); i < rf.logEnd(); i++ {
			if rf.logAt(i).Term == rf.logAt(args.PrevLogIndex).Term {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// sometimes args.PrevLogIndex have lower index. Skip and wait until sendSnapshot finish
	defer func() { recover() }()

	// 3. if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all thatfollow it (§5.3)
	thisLogFirst := rf.logRange(rf.logBegin(), args.PrevLogIndex+1)
	thisLogSecond := rf.logRange(args.PrevLogIndex+1, rf.logEnd())

	DPrintf("%s %v | %v", rf, thisLogFirst, thisLogSecond)
	conflict := false
	for i := 0; i < len(thisLogSecond) && i < len(args.Entries); i++ {
		if thisLogSecond[i].Term != args.Entries[i].Term {
			conflict = true
			break
		}
	}

	if len(args.Entries) > len(thisLogSecond) {
		conflict = true
	}

	// 4. append any new entries not already in the log
	if conflict {
		rf.logs = append(thisLogFirst, args.Entries...)
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
	rf.resetTerm(args.Term, args.LeaderID)
	DPrintf("%s updated Logs: %v", rf, rf.logs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.currentRole != RoleLeader || rf.currentTerm != args.Term {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.resetTerm(reply.Term, NullPeer)
		return false
	}

	return true
}

func (rf *Raft) sendLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s SendLogs: %v", rf, rf.logs)

	if rf.currentRole == RoleLeader {
		// matches on myself
		rf.matchIndex[rf.me] = rf.lastIndex()
		DPrintf("%s matchIndex %v", rf, rf.matchIndex)
		for i := range rf.peers {
			if i != rf.me {
				// need to prepare args inside the mutex
				index := rf.nextIndex[i] - 1
				//				DPrintf("%s nextindex %v", rf, rf.nextIndex)

				// Have no logs privous to rf.lastIncludedIndex => send snapshot
				if index < rf.lastIncludedIndex {
					go rf.sendSnapshot(i)
					continue
				}

				term := 0
				if index >= 0 {
					DPrintf("%s nextindex %v, %d", rf, rf.nextIndex, index)
					term = rf.logAt(index).Term
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: index,
					PrevLogTerm:  term,
					Entries:      rf.logRange(index+1, rf.logEnd()),
				}

				go func(peer int, args AppendEntriesArgs) {
					var reply AppendEntriesReply

					if ok := rf.sendAppendEntries(peer, &args, &reply); !ok {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					// If last log index ≥ nextIndex for a follower: sendAppendEntries RPC with log entries starting at nextIndex
					// If successful: update nextIndex and matchIndex forfollower (§5.3)
					// If AppendEntries fails because of log inconsistency:decrement nextIndex and retry (§5.3)
					// ok
					if reply.Success {
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
					} else {
						// failed, mark to unconflicted index
						rf.nextIndex[peer] = reply.ConflictIndex
					}

					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:set commitIndex = N (§5.3, §5.4).
					for N := rf.lastIndex(); N > rf.commitIndex; N-- {
						count := 0
						DPrintf("%s %v %d", rf, rf.matchIndex, N)
						for p := range rf.peers {
							if rf.matchIndex[p] >= N {
								count++
							}
						}

						DPrintf("%s count: %d, Term: %d", rf, count, rf.logAt(N).Term)
						if count > len(rf.peers)/2 && rf.logAt(N).Term == rf.currentTerm {
							rf.commitIndex = N
							go rf.Commit()
							break
						}
					}
				}(i, args)
			}
		}
	}
}
