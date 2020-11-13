package raft

import "sync"
import "sync/atomic"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	reply.VoteGranted = false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	// reply false if term < currentTerm (§5.1)
	// tell the peer: you are too old
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.resetTerm(args.Term, NullPeer)
	}

	// voted yet ?
	if rf.votedFor != NullPeer && rf.votedFor != args.CandidateID {
		return
	}

	// not up-to-date
	if rf.lastTerm() > args.LastLogTerm ||
		(rf.lastTerm() == args.LastLogTerm &&
			rf.lastIndex() > args.LastLogIndex) {
		return
	}

	DPrintf("%s Vote granted to %d", rf, args.CandidateID)
	reply.VoteGranted = true
	rf.resetTerm(args.Term, args.CandidateID)
	rf.granted <- args.CandidateID
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole != RoleCandidate ||
		rf.currentTerm != args.Term {
		return false
	}

	if reply.Term > rf.currentTerm {
		defer rf.persist()
		rf.resetTerm(reply.Term, NullPeer)
		return false
	}

	return true
}

func (rf *Raft) getAllVote() chan bool {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	DPrintf("%s Start voting", rf)
	rf.persist()
	rf.mu.Unlock()

	//	replyCh := make(chan bool, len(rf.peers)+1)
	//	replyCh <- true // vote for me
	var voteCount, half int32
	voteCount = 1
	half = int32(len(rf.peers) / 2)
	var wg sync.WaitGroup
	isLeader := make(chan bool)
	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go func(peer int) {
				defer wg.Done()
				var reply RequestVoteReply
				if ok := rf.sendRequestVote(peer, args, &reply); !ok {
					return
				}

				if reply.VoteGranted {
					if atomic.AddInt32(&voteCount, 1) > half {
						isLeader <- true
						DPrintf("%s End voting", rf)
						return
					}
				}
			}(i)
		}
	}

	return isLeader
}
