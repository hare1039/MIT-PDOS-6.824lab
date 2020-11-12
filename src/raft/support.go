package raft

import "time"
import "math/rand"

func randElectTime() time.Duration {
	gap := time.Duration(rand.Intn(25)*20) * time.Millisecond
	return 700*time.Millisecond + gap
}

func (rf *Raft) logLength() int {
	return len(rf.logs)
}

func (rf *Raft) lastIndex() int {
	return rf.logLength() - 1
}

func (rf *Raft) lastTerm() int {
	return rf.logs[rf.lastIndex()].Term
}
