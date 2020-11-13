package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"reflect"
	"sync"
	//	"github.com/sasha-s/go-deadlock"
	"time"
)

// build for lab3 again again again
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command  string
	Key      string
	Value    string
	ClientID int64
	Sequence int32
}

func (op Op) String() string {
	//	return fmt.Sprintf("%d %d| %s %s:%q", op.ClientID, op.Sequence, op.Command, op.Key, op.Value)
	return fmt.Sprintf("%s %s:%q by %v, %v", op.Command, op.Key, op.Value, op.ClientID, op.Sequence)
}

type KVServer struct {
	mu sync.Mutex
	//mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	running  chan bool
	kvstore  map[string]string
	resultCh map[int]chan Op
	dupCheck map[int64]int32 // may have dup request (Test: unreliable net, many clients (3A))
}

func (kv *KVServer) String() string {
	s := fmt.Sprintf("[%d %s]", kv.me, kv.rf)
	return s
}

func (kv *KVServer) atResultCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.resultCh[index]; ok {
		return val
	} else {
		ch := make(chan Op, 100)
		kv.resultCh[index] = ch
		return ch
	}
}

func (kv *KVServer) startRaft(op Op) bool {
	isLeader := make(chan bool)
	startResult := make(chan bool)

	go func() {
		//		DPrintf("%s start %s", kv, op)
		//		defer DPrintf("%s end %s", kv, op)
		index, _, leader := kv.rf.Start(op)

		if !leader {
			isLeader <- false
			return
		}

		ch := kv.atResultCh(index)

		select {
		case returnedOp := <-ch:
			defer func() { recover() }()
			close(ch)
			//			DPrintf("%s ch returned. %s, %t", kv, returnedOp, reflect.DeepEqual(returnedOp, op))
			startResult <- reflect.DeepEqual(returnedOp, op)
		}
	}()

	//	DPrintf("%s waits result of %s", kv, op)
	select {
	case ok := <-startResult:
		close(startResult)
		//		DPrintf("%s result ok", kv)
		return ok
	case ans := <-isLeader:
		//		DPrintf("%s isleader %t", kv, ans)
		return ans
	case <-time.After(350 * time.Millisecond):
		//		DPrintf("%s start raft timeout", kv)
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// default error
	op := Op{Command: "Get", Key: args.Key}
	//	DPrintf("%s %s", kv, op)
	reply.IsLeader = false
	if !kv.startRaft(op) {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.kvstore[args.Key]

	if !ok {
		return
	}
	reply.IsLeader = true
	reply.Value = val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.IsLeader = false
	op := Op{
		Command:  args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Sequence: args.Sequence,
	}
	//	DPrintf("%s do %s", kv, op)

	if !kv.startRaft(op) {
		return
	}

	reply.IsLeader = true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.running <- false
}

func (kv *KVServer) service() {
	for {
		var commitedMsg raft.ApplyMsg

		select {
		case <-kv.running:
			return
		case commitedMsg = <-kv.applyCh:
			// continue
		}

		if !commitedMsg.CommandValid {
			snapshot := commitedMsg.Command.([]byte)
			kv.installSnapshot(snapshot)
			DPrintf("%s snapshot installed %#v", kv, kv.kvstore)
			continue
		}

		op := commitedMsg.Command.(Op)

		rsch := kv.atResultCh(commitedMsg.CommandIndex)

		//		DPrintf("%s got op: %s", kv, op)

		kv.mu.Lock()
		seq, ok := kv.dupCheck[op.ClientID]
		if !ok /* not exist the op.ClientID */ ||
			op.Sequence > seq /* have large sequence */ {
			kv.dupCheck[op.ClientID] = op.Sequence

			switch op.Command {
			case "Get":
				// pass
			case "Put":
				kv.kvstore[op.Key] = op.Value
			case "Append":
				kv.kvstore[op.Key] += op.Value
			}
		}
		kv.checkSnapshot(commitedMsg.CommandIndex)
		rsch <- op
		//		DPrintf("%s after execution %#v", kv, kv.kvstore)
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultCh = make(map[int]chan Op)
	kv.kvstore = make(map[string]string)
	kv.running = make(chan bool)
	kv.dupCheck = make(map[int64]int32)

	kv.installSnapshot(kv.persister.ReadSnapshot())

	go kv.service()
	// You may need initialization code here.

	return kv
}
