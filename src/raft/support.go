package raft

import "time"
import "math/rand"

func randElectTime() time.Duration {
	gap := time.Duration(rand.Intn(25)*20) * time.Millisecond
	return 400*time.Millisecond + gap
}

func (rf *Raft) logAt(index int) LogEntry {
	return rf.logs[rf.logVIndex(index)]
}

func (rf *Raft) logLength() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) logBegin() int {
	return rf.lastIncludedIndex
}

func (rf *Raft) logEnd() int {
	return rf.logLength()
}

func (rf *Raft) logRange(begin int, end int) []LogEntry {
	return rf.logs[rf.logVIndex(begin):rf.logVIndex(end)]
}

// returns corrected index
func (rf *Raft) logVIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) lastIndex() int {
	return rf.logLength() - 1
}

func (rf *Raft) lastTerm() int {
	return rf.logAt(rf.lastIndex()).Term
}
