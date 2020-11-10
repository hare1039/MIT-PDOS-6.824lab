package raft

import "log"
import "fmt"

// Debugging
const Debug = 1

func (rf *Raft) String() string {
	roleStr := ""
	switch rf.currentRole {
	case RoleLeader:
		roleStr = "L"
	case RoleFollower:
		roleStr = "F"
	case RoleCandidate:
		roleStr = "C"
	}
	s := fmt.Sprintf("[%d(%s) T%2d V%2d]", rf.me, roleStr, rf.currentTerm, rf.votedFor)
	return s
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
