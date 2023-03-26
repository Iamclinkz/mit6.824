package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServerOp struct {
	Term int
	ID   OpID
	Type string
	K    string
	V    string
}

//KVServer 可以看raft交互图：http://nil.csail.mit.edu/6.824/2021/notes/raft_diagram.pdf
//每一个 KVServer 表示一个kv数据库实体，逻辑上是一个状态机。即如果对所有的分布式kv数据库，依次的执行同样的命令，那么最终
//所有的数据库都是一致的。每个 KVServer 关联了一个 raft 实体，KVServer们并不直接彼此交流，而是通过关联的raft。通过raft.Start()
//向raft输入希望同步的，对状态机的指令，并且从 raft.applyCh中拿到指令，并按照顺序输入的状态机中。
//按照向上暴露的功能来说，Client（即Clerk）可以通过调用 KVServer 的向外暴露的rpc，对数据库实体进行crud操作。
//碍于KVServer的功能实现，客户端执行如果希望执行某个操作，需要调用 KVServer 的rpc，如果这个rpc调用成功，那么一定成功，而如果失败，
//则可能是因为超时，KVServer的rpc返回，但是实际上返回后，KVServer执行成功（即rpc返回失败，但是实际上执行成功）。这种情况下，
//Client（即Clerk）和KVServer均需要做额外处理：
//对于Client，如果rpc返回处理失败，需要重新的向另一个server提交请求，直到在某个server处理成功。
//对于KVServer，应该让Client对请求编号，并且将编号记录，防止同一个编号执行两遍。
//一开始考虑过，让Clerk不编号，放到KVServer中，让某个来自Clerk的指令，执行成功一定返回true，执行失败一定返回false，后来感觉实现不了。。。
//所以没办法，只能在Clerk处给每个希望执行的指令编号
type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//kvMu		sync.Mutex				//因为只有单线程访问myKV，所以这里先不加锁试一下
	myKV map[string]string //实际存放kv的map

	notifyChMu  sync.Mutex
	notifyChMap map[int]chan *applyReply //key为int64的OP.me，value为通知该OP完成的chan，如果error不为""，说明出错
	closeCh     chan struct{}

	finishedOpIDMap map[OpID]struct{} //用于记录某个操作是否已经被执行的map。防止重复执行
	persister       *raft.Persister
}

//applyReply HandleApplyCh go程回复的，对于某次rpc投送的内容的回复
type applyReply struct {
	ok    bool   //是否成功应用到raft
	value string //如果是get，那么此字段为value的值
}

//AddToNotifyMap 将commitIdx的位置，创建一个chan *applyReply
//注意，该位置可能已经又一个chan *applyReply了，但是我们只有在是leader的时候，加锁调用rf.Start()，并且使用
//返回的commitIdx作为key创建，这样保证了如果该位置已经有内容了，说明该位置的内容一定是老的term的commitIdx
//（可能发生在例如term1 我们是leader，在位置1的位置上append了一条log，但是term2我们不是leader了，刚刚的位置1的append
//的日志也因为没有及时传播，被删除了，但是term3的时候我们又是leader了，然后又append到位置1一条日志）
func (kv *KVServer) AddToNotifyMap(commitIdx int) chan *applyReply {
	ch := make(chan *applyReply, 1)
	kv.notifyChMap[commitIdx] = ch
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.log(dLog, "receive get request from client: %+v", *args)

	// Your code here.
	var (
		isLeader  = false
		commitIdx = 0
		notifyCh  chan *applyReply
		chReply   *applyReply
		myOp      KVServerOp //我创建的op
	)

	myOp = KVServerOp{
		Type: "Get",
		K:    args.Key,
		V:    "",
	}

	//todo 是否有死锁？
	kv.notifyChMu.Lock()
	//投送到raft中，拿到一个commitIdx，如果本指令被提交，那么只能被提交到commitIdx，
	//逆否命题为：没有提交到commitIdx->本指令没有被提交
	//所以我们只需要看一下，提交到commitIdx的指令，是不是本指令即可
	commitIdx, _, isLeader = kv.rf.Start(myOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.notifyChMu.Unlock()
		kv.log(dLog, "fail to handle get request from client: %+v, wrong leader, ×", *args)
		return
	}
	notifyCh = kv.AddToNotifyMap(commitIdx)
	kv.log(dLog, "add request to notifyMap, idx:%v", commitIdx)
	kv.notifyChMu.Unlock()

	select {
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		kv.log(dLog, "fail to handle get request from client: %+v, timeout, ×", *args)
		return
	case chReply = <-notifyCh:
		kv.log(dLog, "receive chReply from notifyCh: %+v", chReply)
	}

	//代码执行到这里，commitIdx已经产生了一条raft达成共识的cmd，并且我们在HandleApplyCh中设定了，
	//只有指令中的term == myTerm 才算是我们的指令，所以不需要怀疑，一定是我们在本term投送给我们的raft的指令，
	//且本raft仍然为leader
	if chReply.ok {
		reply.Err = OK
		reply.Value = chReply.value
		kv.log(dLog, "successful handled get request from client: %+v, reply:%+v, √", *args, *reply)
	} else {
		reply.Err = ErrNoKey
		reply.Value = chReply.value
		kv.log(dLog, "fail to handle get request from client: %+v, err no key, ×", *args)
	}

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	// Your code here.
	var (
		isLeader  = false
		commitIdx = 0
		notifyCh  chan *applyReply
		chReply   *applyReply
		myOp      KVServerOp //我创建的op
	)
	kv.log(dLog, "receive putAppend request from client: %+v", *args)

	myOp = KVServerOp{
		Type: args.Type,
		K:    args.Key,
		V:    args.Value,
		ID:   args.OpID,
	}

	//todo 是否有死锁？
	//这里为了防止我们刚扔到rf.Start中，还没把我们自己注册到NotifyMap中，raft就处理完成，并且go程切换，
	//将我们这次cmd通过applyCh传到了HandleApplyCh go程，这里采取了一种暴力的方式，直接加一个大锁
	kv.notifyChMu.Lock()
	//投送到raft中，拿到一个commitIdx，如果本指令被提交，那么只能被提交到commitIdx，
	//逆否命题为：没有提交到commitIdx->本指令没有被提交
	//所以我们只需要看一下，提交到commitIdx的指令，是不是本指令即可
	commitIdx, _, isLeader = kv.rf.Start(myOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.notifyChMu.Unlock()
		kv.log(dLog, "fail to handle putAppend request from client: %+v, wrong leader, ×", *args)
		return
	}
	notifyCh = kv.AddToNotifyMap(commitIdx)
	kv.log(dLog, "add request to notifyMap, idx:%v", commitIdx)
	kv.notifyChMu.Unlock()

	select {
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		kv.log(dLog, "fail to handle putAppend request from client: %+v, timeout, ×", *args)
		return
	case chReply = <-notifyCh:
		kv.log(dLog, "receive chReply from notifyCh: %+v", chReply)
	}

	//代码执行到这里，commitIdx已经产生了一条raft达成共识的cmd，并且我们在HandleApplyCh中设定了，
	//只有指令中的term == myTerm 才算是我们的指令，所以不需要怀疑，一定是我们在本term投送给我们的raft的指令，
	//且本raft仍然为leader
	if chReply.ok {
		kv.log(dLog, "successful handled putAppend request from client: %+v, reply:%+v, √", *args, *reply)
		reply.Err = OK
	} else {
		kv.log(dInfo, "putAppend rpc should not receive fall msg!")
		panic("")
	}

	return
}

//AppendOrPut 如果以前做过，那么不重复做
func (kv *KVServer) AppendOrPut(opType, key, value string, id OpID) {
	if _, ok := kv.finishedOpIDMap[id]; ok {
		return
	}

	switch opType {
	case "Append":
		kv.myKV[key] = kv.myKV[key] + value
	case "Put":
		kv.myKV[key] = value
	}

	kv.finishedOpIDMap[id] = struct{}{}
}

//HandleApplyCh 监听来自raft的applyCh，并且应用到实际的kv数据库中
func (kv *KVServer) HandleApplyCh() {
	var (
		op         KVServerOp
		msg        raft.ApplyMsg
		ok         bool
		replyValue string
	)
	for !kv.killed() {
		select {
		case msg = <-kv.applyCh:
			kv.log(dCommit, "receive new commit: %+v", msg)
			if msg.SnapshotValid {
				//如果是压缩日志请求，那么执行压缩日志
				if ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot); ok {
					kv.loadFromSnapshot(msg.Snapshot)
				}
				continue
			}

			//检查是否可以转换程KVServerOp，如果不可以则报错
			if op, ok = msg.Command.(KVServerOp); !ok {
				kv.log(dError, "receive msg which can not convert to KVServerOp")
				continue
			}

			//如果是普通的日志
			switch op.Type {
			case "Get":
				//如果是get，不记录到finishedOpIDMap中，直接执行
				replyValue = kv.myKV[op.K]
			default:
				//记录 + 执行
				kv.AppendOrPut(op.Type, op.K, op.V, op.ID)
				replyValue = ""
			}

			if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
				//如果我是leader，并且command的term等于当前的term，说明是我的rpc过程中调用start提交的，需要给我的rpc一个回复
				kv.notifyChMu.Lock()
				if ch := kv.notifyChMap[msg.CommandIndex]; ch == nil {
					//有可能从故障中恢复时，由于没有缓存notifyMap，所以notifyMap被重置，这里也不会取的出内容
				} else {
					ch <- &applyReply{
						ok:    true,
						value: replyValue,
					}
					kv.log(dCommit, "successful pushed applyReply to notifyCh, idx:%v", msg.CommandIndex)
					kv.notifyChMu.Unlock()
				}
			}

			//检查一手raft的日志是不是过量，如果过量，需要压缩一个快照，并且用快照代替原来的日志
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.makeSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}
		case <-kv.closeCh:
			//todo 清理一下
			kv.log(dWarn, "kv.closeCh is closed, HandleApplyCh stop working...")
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(KVServerOp{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	if ok := kv.loadFromSnapshot(persister.ReadSnapshot()); !ok {
		kv.myKV = make(map[string]string)
		kv.finishedOpIDMap = make(map[OpID]struct{})
	}
	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	// You may need initialization code here.

	//todo 初始化各种ch
	kv.notifyChMu = sync.Mutex{}
	kv.notifyChMap = make(map[int]chan *applyReply)
	kv.closeCh = make(chan struct{})

	go kv.HandleApplyCh()
	return kv
}
