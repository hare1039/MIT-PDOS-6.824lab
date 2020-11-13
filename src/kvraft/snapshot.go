package raftkv

import "bytes"
import "labgob"

func (kv *KVServer) checkSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	if float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.85 {
		go kv.rf.StartSnapshotOn(index, kv.snapshot())
	}
}

func (kv *KVServer) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvstore)
	e.Encode(kv.dupCheck)
	return w.Bytes()
}

func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvstore map[string]string
	var dupCheck map[int64]int32
	d.Decode(&kvstore)
	d.Decode(&dupCheck)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvstore = kvstore
	kv.dupCheck = dupCheck
	return
}
