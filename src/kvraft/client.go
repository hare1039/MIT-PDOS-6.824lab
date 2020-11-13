package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientID int64
	sequence int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.sequence = 0
	ck.clientID = nrand()
	ck.leader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	for {
		var reply GetReply
		ok := ck.servers[ck.leader].Call("KVServer.Get", &GetArgs{Key: key}, &reply)

		if ok && reply.IsLeader {
			return reply.Value
		}
		ck.nextLeader()
	}

	return ""
}

func (ck *Clerk) nextLeader() {
	ck.leader++
	if ck.leader >= len(ck.servers) {
		ck.leader = 0
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	defer DPrintf("Clerk %s %s:%s finiehed", op, key, value)
	ck.sequence++
	for {
		time.Sleep(0 * time.Millisecond)
		args := PutAppendArgs{
			ClientID: ck.clientID,
			Sequence: ck.sequence,
			Op:       op,
			Key:      key,
			Value:    value,
		}

		DPrintf("Clerk => %d: %s %s:%s waiting reply", ck.leader, op, key, value)
		var reply PutAppendReply
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)

		DPrintf("Clerk => %d: %s %s:%s replied: %t %t", ck.leader, op, key, value, ok, reply.IsLeader)

		if ok && reply.IsLeader {
			return
		}
		ck.nextLeader()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
