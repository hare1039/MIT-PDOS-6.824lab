package raft

import "time"
import "math/rand"

func randElectTime() time.Duration {
	gap := time.Duration(rand.Intn(25)*20) * time.Millisecond
	return 400*time.Millisecond + gap
}

func (rf *Raft) lastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) lastTerm() int {
	return rf.logs[rf.lastIndex()].Term
}
