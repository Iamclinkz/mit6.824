package kvraft

import (
	"6.824/labrpc"
	"log"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

const DoLog = true

func Log(format string, a ...interface{}) {
	if DoLog {
		log.Printf(format, a...)
	}
	return
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me          int64
	SequenceNum int64
	leaderID    int32
}

func (ck *Clerk) GenSequenceNum() OpID {
	return OpID{
		ClientSequenceNum: atomic.AddInt64(&ck.SequenceNum, 1),
		ClientID:          ck.me,
	}
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
	ck.me = nrand()
	return ck
}

func (ck *Clerk) SetLeaderID(leaderID int) {
	atomic.StoreInt32(&ck.leaderID, int32(leaderID))
}

func (ck *Clerk) GetLeaderID() int {
	return int(atomic.LoadInt32(&ck.leaderID))
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
	args := &GetArgs{Key: key}
	reply := &GetReply{}
	leaderID := ck.GetLeaderID()

	for {
		for i := leaderID; i < len(ck.servers); i++ {
			Log("Clerk(%v) send get request to S%v", ck.me, i)
			ok := ck.servers[i].Call("KVServer.Get", args, reply)
			if !ok {
				Log("Clerk(%v) timeout to send get request to S%v", ck.me, i)
				continue
			}

			if reply.Err != OK {
				Log("Clerk(%v) get request to S%v has failed, reason:%v", ck.me, i, reply.Err)
				continue
			}

			Log("Clerk(%v) get request to S%v ok", ck.me, i)
			ck.SetLeaderID(i)
			return reply.Value
		}

		leaderID = 0
		time.Sleep(10 * time.Millisecond)
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
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Type:  op,
		OpID:  ck.GenSequenceNum(),
	}
	reply := &PutAppendReply{}
	leaderID := ck.GetLeaderID()

	for {
		for i := leaderID; i < len(ck.servers); i++ {
			Log("Clerk(%v) send PutAppend request to S%v", ck.me, i)
			ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
			if !ok {
				Log("Clerk(%v) timeout to send get request to S%v", ck.me, i)
				continue
			}

			if reply.Err != OK {
				Log("Clerk(%v) get request to S%v has failed, reason:%v", ck.me, i, reply.Err)
				continue
			}

			Log("Clerk(%v) get request to S%v has ok", ck.me, i)
			ck.SetLeaderID(i)
			return
		}

		leaderID = 0
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
