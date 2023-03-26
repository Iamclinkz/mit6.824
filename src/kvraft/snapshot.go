package kvraft

import (
	"6.824/labgob"
	"bytes"
)

//AppendOrPut 如果以前做过，那么不重复做
func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.myKV); err != nil {
		kv.log(dError, "failed to persist kv.myKV, len:%v", len(kv.myKV))
		panic("")
	}

	if err := e.Encode(kv.finishedOpIDMap); err != nil {
		kv.log(dError, "failed to persist kv.finishedOpIDMap, len:%v", len(kv.finishedOpIDMap))
		panic("")
	}

	data := w.Bytes()
	kv.log(dPersist, "finish making snapshot, len:%v", len(data))
	return data
}

func (kv *KVServer) loadFromSnapshot(data []byte) bool {
	if data == nil || len(data) < 1 {
		return false
	}

	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.myKV) != nil {
		kv.log(dError, "failed to load term from persist")
		panic("")
	}

	if d.Decode(&kv.finishedOpIDMap) != nil {
		kv.log(dWarn, "failed to load votedFor from persist")
		panic("")
	}

	kv.log(dPersist, "finish load from snapshot, len of myKV:%v, len of finishOpIDMap:%v",
		len(kv.myKV), len(kv.finishedOpIDMap))

	return true
}
