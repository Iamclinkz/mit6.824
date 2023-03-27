package kvraft

import (
	"6.824/labgob"
	"bytes"
)

//makeSnapshot 序列化一手当前的状态
func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	//这里主要保存的有两个，一个是当前的kv，用于还原出当前状态下的数据库，
	//另一个是当前的finishedOpIDMap，用于还原出，当前状态下的数据库是经过了哪些操作（执行每个Clerk的操作）到了现在的数据库的样子
	//这样即使不知道我们状态机当前apply到哪一条指令（或者说不知道raft的日志的index，apply到了多少），即使从index0开始执行，也是可以
	//完整的还原出当前的内容，且不会重复执行的。
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
