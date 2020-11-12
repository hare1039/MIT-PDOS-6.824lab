package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"reflect"
	"sync"
	"time"
)

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
	return fmt.Sprintf("%s %s:%q", op.Command, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	running  bool
	kvstore  map[string]string
	resultCh map[int]chan Op
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
		ch := make(chan Op, 1)
		kv.resultCh[index] = ch
		return ch
	}
}

func (kv *KVServer) startRaft(op Op) (Op, bool) {

	isLeader := make(chan bool)
	startResult := make(chan bool)

	go func() {
		DPrintf("%s start %s", kv, op)
		defer DPrintf("%s end %s", kv, op)
		index, _, leader := kv.rf.Start(op)

		if !leader {
			isLeader <- false
			return
		}

		kv.mu.Lock()
		ch := kv.atResultCh(index)
		kv.mu.Unlock()

		select {
		case returnedOp := <-ch:
			close(ch)
			startResult <- reflect.DeepEqual(returnedOp, op)
		}
	}()

	DPrintf("%s waits result", kv)
	select {
	case ok := <-startResult:
		close(startResult)
		return Op{}, ok

	case <-isLeader:
		return Op{}, false
	case <-time.After(700 * time.Millisecond):
		return Op{}, false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// default error
	op := Op{Command: "Get", Key: args.Key}
	DPrintf("%s %s", kv, op)
	reply.IsLeader = false
	_, ok := kv.startRaft(op)
	if !ok {
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
	DPrintf("%s do %s", kv, op)

	_, ok := kv.startRaft(op)

	if !ok {
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
	kv.running = false
}

func (kv *KVServer) service() {
	for kv.running {
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		DPrintf("%s got op: %s", kv, op)
		kv.mu.Lock()
		kv.atResultCh(msg.CommandIndex) <- op
		switch op.Command {
		case "Get":
			// pass
		case "Put":
			kv.kvstore[op.Key] = op.Value
		case "Append":
			kv.kvstore[op.Key] += op.Value
		}
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
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultCh = make(map[int]chan Op)
	kv.running = true

	go kv.service()
	// You may need initialization code here.

	return kv
}
