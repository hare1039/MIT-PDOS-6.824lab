package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Sequence int32
}

// change WrongLeader to IsLeader. IsLeader is easier to understand
type PutAppendReply struct {
	IsLeader bool
	Err      Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	IsLeader bool
	Err      Err
	Value    string
}
