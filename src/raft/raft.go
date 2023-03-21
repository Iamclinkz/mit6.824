package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"

	"6.824/labgob"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh      chan ApplyMsg //提交给上层的log entry
	applyTransCh chan ApplyMsg

	closeCh chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int32 //当前的轮次

	votedFor int //本轮投给谁了，注意这个字段需要和leader保持相同

	//保护下面的几个字段
	//leaderMu      sync.Mutex
	currentLeader int

	//log 有关
	//logMu sync.Mutex  //下面几个字段日志相关的字段的锁

	commandCh chan *CommandWithNotifyCh //只有leader使用，来自客户端的LogEntry
	//下面两个字段的关系可以见 https://www.zhihu.com/question/61726492/answer/190736554
	//commitIndex 是本raft感知到(commit)的最后一条log entry的index
	//lastApplied 是本raft所在的状态机器最后一条应用的index
	commitIndex int //已经提交的最后一条log entry的index，注意不是rf.logs的下标，而是日志的编号！因为有了哨兵所以两者不一样！
	commitTerm  int //已经提交的最后一条log entry的term。注意commit != apply

	lastApplied int //已经被state machine应用的最后一条log entry的index
	//follower使用，本term是否已经match到leader了。在测试100000次时发现的错误，leader如果在add一条command之前，发了一个没有任何条目
	//的rpc（称为A）给某个follower，随后leader添加了一个条目后，又发给了该follower一个rpc（称为B），则如果AB乱序，可能新的那个条目可能
	//一开始被保存到该follower上，并且该follower返回ok，但是接下来又将该条目删除。从而leader以为该follower保存成功，实际上没有。容易出现
	//commit的错误。所以加了这个字段，如果本term已经跟leader match了，那么就不会再删除字段了
	thisTermMatchedLeader bool

	//leader使用，一定要注意！！下面的三个字段都表示的是rf.logs的下标，而非log的编号！
	//第n条log被放置在rf.Logs[n+1]的位置上！！！
	//index为peer的index，value为下一个应该发送的log entry的下标（开始为leader的last log + 1，也就是len(rf.Logs)）
	nextIndex []int
	//index为peer的index，value为该peer已经复制并且得到了对方的确认的log entry的下标（开始为0），可以用这个数组来计算commitIndex
	matchIndex      []int
	minLogNextIndex int //当前几个follower中，最小的nextIndex的值

	//go routine相关
	wg *sync.WaitGroup

	//当前状态，只允许主go程set，其他go程可以get
	state                     State
	CandidateStateHandlerInst CandidateStateHandler
	LeaderStateHandlerInst    LeaderStateHandler
	FollowerStateHandlerInst  FollowerStateHandler
	//因为只在主go程中访问，所以不需要加锁
	CurrentStateHandler StateHandler

	//心跳/附加日志rpc
	lastHeartBeatTime    int64                       //上次heartBeat的时间
	needHeartBeat        chan struct{}               //需要发送心跳
	appendEntriesReqCh   chan *rpcChMsg              //附加日志请求chan
	appendEntriesReplyCh chan *AppendEntriesReplyMsg //附加日志回复chan

	//竞选rpc相关
	requestVoteReqCh   chan *rpcChMsg
	requestVoteReplyCh chan *RequestVoteReply //请求投票回复chan
	//voterMu sync.Mutex
	//todo 只在主线程统计，所以暂时不用lock了
	needElectionCh        chan struct{} //需要参与竞选
	voter                 map[int]struct{}
	candidateOverTimeTick <-chan time.Time
	leastVoterNum         int

	//2D snapshot
	//LastIncludeIndex int    //原本的rf.logs的 [0,LastIncludeIndex] 都被当前的snapshot给替代了，实际放到了rf.logEntries.lastIncludeIndex中
	//lastIncludeTerm  int    //最后一条被snapshot(下标为lastIncludeIndex的日志)替代的日志的term，实际放到了rf.logEntries[0].Term中
	logEntries               *LogEntries           //2D：将raft的日志项封装成一个结构了
	snapshot                 []byte                //[0,LastIncludeIndex] 的日志的快照
	snapshotCh               chan *SnapshotRequest //存放Snapshot()请求的request的chan
	condInstallSnapshotMsgCh chan *CondInstallSnapshotMsg
	//安装快照rpc
	installSnapshotReqCh   chan *rpcChMsg
	installSnapshotReplyCh chan *InstallSnapshotReplyMsg
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(rf.getTerm()); err != nil {
		rf.log(dWarn, "failed to persist currentTerm: %v", rf.getTerm())
		return
	}

	if err := e.Encode(rf.getVotedFor()); err != nil {
		rf.log(dWarn, "failed to persist votedFor: %v", rf.getVotedFor())
		return
	}

	if err := e.Encode(rf.logEntries); err != nil {
		rf.log(dWarn, "failed to persist current Logs, len:%v", rf.logEntries.Len())
		return
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.log(dPersist, "successfully persisted information, votedFor:%v, logLen:%v", rf.getVotedFor(), rf.logEntries.Len())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs *LogEntries

	if d.Decode(&term) != nil {
		rf.log(dWarn, "failed to load term from persist")
		return
	}

	if d.Decode(&votedFor) != nil {
		rf.log(dWarn, "failed to load votedFor from persist")
		return
	}

	if d.Decode(&logs) != nil {
		rf.log(dWarn, "failed to load Logs from persist")
		return
	}

	//todo 2D

	//因为readPersist只在开始时调用，所以不需要加锁。注意在readPersister中不应该有rf.Persist()的操作！否则可能死锁！
	atomic.StoreInt32(&rf.currentTerm, int32(term))
	rf.votedFor = votedFor
	rf.logEntries = logs
	rf.log(dPersist, "successful loaded from persist")
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// service层用于通知raft层，希望raft层可以使用snapshot参数表示的快照，替换掉
// 从lastIncludedIndex开始的日志，如果raft觉得可以的话，返回true，否则返回false。
// raft可能会感觉不可以，因为调用CondInstallSnapshot时，raft又在lastIncludedIndex之后加了新的日志。
// 这样如果直接应用该日志，会丢弃掉lastIncludedIndex的日志，所以不可以接受
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	ret := <-rf.sendCondInstallSnapshotMsg(lastIncludedTerm, lastIncludedIndex, snapshot)
	retMsg := "×"
	if ret {
		retMsg = "√ "
	}
	rf.log(dSnap, "CondInstallSnapshot: lastIncludedTerm:%v, lastIncludedIndex:%v, success:%v",
		lastIncludedTerm, lastIncludedIndex, retMsg)
	return ret
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 这部分的理解可以看 http://nil.csail.mit.edu/6.824/2021/notes/raft_diagram.pdf 的图
// raft层上层的service层（也可以理解成一个状态机），会定期的将raft层（通过applyCh）上传的日志进行压缩（例如满100条压缩一次），
// 压缩成快照的形式。（例如raft论文中的图12）
// 压缩完成之后，会通知raft层，然后raft层可以将压缩成功的日志在rf.logs中删除掉，并且用snapshot来代替这些日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//todo 这里要不要让上层调用阻塞一下？
	rf.log(dSnap, "service snapshots until index:%v", index)
	replyCh := rf.pushSnapshot(index, snapshot)
	<-replyCh
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//这里先初步判断一下，如果不是leader，则直接返回，如果是leader，那么可能收到消息的时候已经不是了，
	//但是仍然需要给客户端个返回
	if rf.killed() || !rf.isLeader() {
		//rf.log(dClient,"receive command, but failed to push")
		return -1, -1, false
	}

	notifyCh := rf.pushCommand(command)
	ret := <-notifyCh

	if !ret.ok {
		rf.log(dClient, "receive command, but failed to push")
		return -1, -1, false
	}
	rf.log(dClient, "receive command, and successfully push to idx:%v", ret.idx)
	return ret.idx, ret.term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.closeCh)
	rf.wg.Wait()

	rf.readChAndThrowUntilEmpty()
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// reSetHeartBeat 将心跳重置，并且清空当前needElectionCh中的请求
func (rf *Raft) reSetHeartBeat() {
	now := time.Now().UnixMicro()
	atomic.StoreInt64(&rf.lastHeartBeatTime, now)
	readChAndThrow(rf.needElectionCh)
}

func (rf *Raft) heartBeatExpire() bool {
	lastTime := time.UnixMicro(atomic.LoadInt64(&rf.lastHeartBeatTime))
	if lastTime.Add(getFollowerHeartBeatExpireTime()).Before(time.Now()) {
		return true
	}
	return false
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.initHandler()
	rf.setState(Follower)
	rf.log(dWarn, "begin restart...")

	rf.closeCh = make(chan struct{})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.needElectionCh = make(chan struct{})
	rf.requestVoteReqCh = make(chan *rpcChMsg, 2048)
	rf.appendEntriesReplyCh = make(chan *AppendEntriesReplyMsg, 2048)
	rf.snapshotCh = make(chan *SnapshotRequest, 1024)
	rf.condInstallSnapshotMsgCh = make(chan *CondInstallSnapshotMsg, 1024)
	rf.installSnapshotReqCh = make(chan *rpcChMsg, 1024)
	rf.installSnapshotReplyCh = make(chan *InstallSnapshotReplyMsg, 1024)

	rf.commandCh = make(chan *CommandWithNotifyCh, 2048)
	// Your initialization code here (2A, 2B, 2C).

	rand.Seed(makeSeed())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.appendEntriesReqCh = make(chan *rpcChMsg)
	rf.needHeartBeat = make(chan struct{})
	rf.applyCh = applyCh
	//因为看着上层创建的applyCh的len是0，也就是说如果我们主go程向其中push，有可能阻塞，
	//所以单独搞一个线程，往里面push
	rf.applyTransCh = make(chan ApplyMsg, 2048)
	rf.snapshotCh = make(chan *SnapshotRequest, 2028)

	if rf.logEntries == nil {
		rf.logEntries = NewLogEntries()
		rf.logEntries.Reinit(-1, 0)
	}

	//rf.stopCandidateCh = make(chan struct{},1)

	rf.leastVoterNum = len(peers)/2 + 1
	rf.log(dInfo, "request voterNum:%v", rf.leastVoterNum)

	rf.requestVoteReplyCh = make(chan *RequestVoteReply, len(rf.peers))

	// start startFollowerHeartBeatCheckTicker goroutine to start elections

	rf.wg = &sync.WaitGroup{}

	rf.goF(rf.startFollowerHeartBeatCheckTicker, "FollowerHeartBeatCheckTicker")
	rf.goF(rf.startLeaderHeartBeatTicker, "LeaderHeartBeatTicker")
	rf.goF(rf.startMainLoop, "Main Loop")
	rf.goF(rf.pushToReplyCh, "pushToReplyCh")

	return rf
}

func (rf *Raft) startMainLoop() {
	for !rf.killed() {
		select {
		//处理请求投票rpc
		case chMsg := <-rf.requestVoteReqCh:
			var req *RequestVoteArgs
			var resp *RequestVoteReply
			var ok bool
			if req, ok = chMsg.RpcReq.(*RequestVoteArgs); !ok {
				panic("program fault")
			}
			if resp, ok = chMsg.RpcResp.(*RequestVoteReply); !ok {
				panic("program fault")
			}
			err := rf.CurrentStateHandler.HandleRequestVote(req, resp)
			chMsg.finish(err)

		//请求追加日志条目（心跳）rpc
		case chMsg := <-rf.appendEntriesReqCh:
			var req *AppendEntriesArgs
			var resp *AppendEntriesReply
			var ok bool
			if req, ok = chMsg.RpcReq.(*AppendEntriesArgs); !ok {
				panic("program fault")
			}
			if resp, ok = chMsg.RpcResp.(*AppendEntriesReply); !ok {
				panic("program fault")
			}
			err := rf.CurrentStateHandler.HandleAppendEntries(req, resp)
			chMsg.finish(err)

		//安装快照rpc
		case chMsg := <-rf.installSnapshotReqCh:
			var req *InstallSnapshotRequest
			var resp *InstallSnapshotRequestReply
			var ok bool
			if req, ok = chMsg.RpcReq.(*InstallSnapshotRequest); !ok {
				panic("program fault")
			}
			if resp, ok = chMsg.RpcResp.(*InstallSnapshotRequestReply); !ok {
				panic("program fault")
			}
			err := rf.CurrentStateHandler.HandleInstallSnapshot(req, resp)
			chMsg.finish(err)

		//来自定时器的心跳超时，应该开启选举事件
		case <-rf.needElectionCh:
			rf.CurrentStateHandler.HandleNeedElection()

		//来自定时器的leader开始发送心跳事件
		case <-rf.needHeartBeat:
			rf.CurrentStateHandler.LeaderHeartBeat()

		//请求投票rpc收到回复
		case reply := <-rf.requestVoteReplyCh:
			rf.CurrentStateHandler.OnRequestVoteReply(reply)

		//竞选者tick超时事件
		case <-rf.candidateOverTimeTick:
			rf.CurrentStateHandler.OnCandidateOverTimeTick()

		case cmd := <-rf.commandCh:
			rf.CurrentStateHandler.OnClientCmdArrive(cmd)

		case reply := <-rf.appendEntriesReplyCh:
			rf.CurrentStateHandler.OnAppendEntriesReply(reply)

		case snapShotReq := <-rf.snapshotCh:
			rf.CurrentStateHandler.HandleSnapshot(snapShotReq)
			close(snapShotReq.ch)

		case reply := <-rf.installSnapshotReplyCh:
			rf.CurrentStateHandler.OnInstallSnapshotRequestReply(reply)

		case req := <-rf.condInstallSnapshotMsgCh:
			req.finishCh <- rf.CurrentStateHandler.HandleCondInstallSnapshot(req.lastIncludedTerm,
				req.lastIncludedIndex, req.snapshot)

		case <-rf.closeCh:
			return
		}
	}
}

// leaderAddCommand 只可以在主go程中使用。往log切片中加入一条command
// 返回把command加入到的位置
func (rf *Raft) leaderAddCommand(command interface{}) (pos int) {
	entry := &LogEntry{
		Command: command,
		Term:    rf.getTerm(),
	}

	pos = rf.logEntries.AppendCommand(entry)
	rf.persist()
	rf.nextIndex[rf.me] = pos + 1
	rf.matchIndex[rf.me] = pos

	if command == nil {
		rf.log(dWarn, "get nil command, idx:%v", pos)
	}

	rf.log(dLog2, "leader add command, idx:%v", pos)
	return
}

// applyLog 只可以在主go程中使用。将 [ lastApplied + 1, to ] 的日志apply，即传递、应用到上层的状态机
func (rf *Raft) applyLog(to int) {
	if to > rf.logEntries.GetLastLogEntryIndex() {
		//如果希望提交的log的index大于当前logs切片的长度
		rf.log(dError, "try to apply log:%v, which is greater than current last index:%v", to, rf.logEntries.GetLastLogEntryTerm())
		panic("")
	}

	if to <= rf.lastApplied {
		//如果重复提交
		rf.log(dError, "try to apply log:%v, which is greater than or equals to rf.commitIndex:%v", to, rf.getLastCommitIdx())
		panic("")
	}

	//应用到上层的状态机
	for i := rf.lastApplied + 1; i <= to; i++ {
		entry := rf.logEntries.Get(i)
		if entry == nil {
			rf.log(dError, "try to apply log idx:%v, which is not contains in current log entries:[%v,%v]",
				i, rf.logEntries.GetFirstLogEntryIndex(), rf.logEntries.GetLastLogEntryIndex())
			panic("")
		}
		rf.applyTransCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i,
		}
		rf.log(dClient, "apply log entry, index: %v,term: %v", i, entry.Term)
	}

	//rf.log(dClient,"apply log entry: %v to %v",rf.lastApplied + 1,to)
	rf.lastApplied = to
}

// updateCommitIndex 只能在主线程调用，计算，并且更新当前的commitIndex
func (rf *Raft) updateCommitIndex() (updated bool) {
	tmp := make([]int, len(rf.peers))

	//将所有的peer的matchIndex统计（自己的matchIndex为len(rf.Logs)-1)，然后找到len(rf.Logs)/2 + 1的位置，即commitIndex
	for i := 0; i < len(rf.peers); i++ {
		tmp[i] = rf.matchIndex[i]
	}

	QuickSort(tmp, 0, len(tmp)-1)

	//如果不只有一个peer，那么选取len(rf.peers)/2 + 1的位置，作为当前的commitIndex
	oldIdx := rf.getLastCommitIdx()
	newCommitIdx := tmp[len(rf.peers)/2]

	//todo bug 明天看一下。。
	if entry := rf.logEntries.Get(newCommitIdx); entry != nil && entry.Term != rf.getTerm() {
		//卡了一天的bug。。5.4.2 leader只能按照超过半数来提交本term的日志，而不能提交之前term的日志
		//换句话说，如果当前更新到的commitIndex所对应的log不是我们本term添加上的，那么就不应该更新
		return false
	}

	if newCommitIdx > rf.getLastCommitIdx() {
		rf.setLastCommitIdx(newCommitIdx)
		rf.log(dCommit, "update current commitIndex: %v -> %v", oldIdx, rf.getLastCommitIdx())
		return true
	}

	return false
}

// logEntriesNewerThanMe 另一个peer的logEntries是否比我的新。来自论文5.4.1
// 如果完全相同，也返回true。因为论文中都是"起码不比我老"这样的陈述
func (rf *Raft) logEntriesNewerThanMe(otherLastLogEntryTerm, otherLastLogEntryIndex, serverID int) bool {
	//注意这里指的是最后一条日志的任期号和索引号，而非commit的索引号和任期号！
	myLastLogEntryTerm := rf.getLastLogEntryTerm()
	//如果最后条目的任期号不同，那么任期号大的更新
	if otherLastLogEntryTerm != myLastLogEntryTerm {
		if otherLastLogEntryTerm > myLastLogEntryTerm {
			rf.log(dTrace, "check S%v's last log term:%v is newer than mine:%v √",
				serverID, otherLastLogEntryTerm, myLastLogEntryTerm)
			return true
		} else {
			rf.log(dTrace, "check S%v's last log term:%v is older than mine:%v ×",
				serverID, otherLastLogEntryTerm, myLastLogEntryTerm)
			return false
		}
	}

	//如果最后的日志条目的任期号相同，那么index大的更新
	myLastLogEntryIndex := rf.getLastLogEntryIndex()
	if myLastLogEntryIndex > otherLastLogEntryIndex {
		rf.log(dTrace, "check S%v's last log index:%v is smaller than mine:%v ×",
			serverID, otherLastLogEntryIndex, myLastLogEntryIndex)
		return false
	}

	rf.log(dTrace, "check S%v's last log index:%v is greater than mine:%v √",
		serverID, otherLastLogEntryIndex, myLastLogEntryIndex)
	return true
}

// doAppendEntry 只能在主线程调用，处理来自当前承认的leader的appendEntry指令，返回是否成功
func (rf *Raft) doAppendEntry(args *AppendEntriesArgs) bool {
	//todo 看一手
	if rf.thisTermMatchedLeader && args.PrevLogIndex+len(args.Entries) <= rf.getLastLogEntryIndex() {
		//如果我们当前已经跟leader匹配了，但是leader发的包仍然没有新的内容，这可能是因为leader没有append新的logEntry，
		//也可能是因为rpc乱序，总之直接return
		//args.PrevLogIndex + len(args.Entries) 的值是leader发来的最后一条日志在rf.logs中的index
		//如果这个位置不如我们的大，那么直接返回，不做处理
		return true
	}

	persist := false
	lastLogEntryIdx := rf.logEntries.GetLastLogEntryIndex()

	if args.PrevLogIndex > lastLogEntryIdx {
		//如果leader发的前一条log的index比我们最后一条log的index都大，那么不接受，直接return false
		rf.log(dLog2, "doAppendEntry failed because PrevLogIndex(%v) > myLastLogEntryIdx(%v)",
			args.PrevLogIndex, lastLogEntryIdx)
		return false
	}

	if entry := rf.logEntries.Get(args.PrevLogIndex); entry != nil {
		if entry.Term != args.PrevLogTerm {
			//如果该entry当前没有被snapshot，且Term不匹配，那么说明自己一定跟leader对不上，直接返回false即可
			return false
		}

		//如果是匹配的，那么直接截断匹配的日志的后面的内容，然后替换成leader的entry
		//截断日志
		newLastLogEntryIdx := rf.logEntries.RemoveCommandUntil(args.PrevLogTerm)
		if lastLogEntryIdx != newLastLogEntryIdx {
			rf.log(dLog2, "delete entry: %v -> %v", newLastLogEntryIdx+1, lastLogEntryIdx)
			persist = true
		}
		lastLogEntryIdx = newLastLogEntryIdx

		//把日志加到当前日志的后面
		newLastLogEntryIdx = rf.logEntries.AppendCommands(args.Entries)
		if lastLogEntryIdx != newLastLogEntryIdx {
			rf.log(dLog2, "add entry: %v -> %v", lastLogEntryIdx+1, newLastLogEntryIdx)
			persist = true
		}

		if persist {
			//2C：如果加了或者减了日志，那么需要持久化
			rf.persist()
		}
		rf.thisTermMatchedLeader = true
		return true
	}

	//如果leader发来的PrevLogIndex，已经被我们snapshot了，因为snapshot肯定被提交过了，而leader一定匹配所有的已经提交的日志，
	//所以自己跟leader的日志一定是匹配的，这种情况下，只需要截断掉我们当前的所有日志，替换成leader的日志即可
	rf.logEntries.Logs = append(rf.logEntries.Logs[:1], args.Entries[rf.logEntries.LastIncludeIndex-args.PrevLogIndex:]...)
	rf.log(dLog, "doAppendEntry use leader's logs, change lastLogIndex: %v -> %v", lastLogEntryIdx, rf.logEntries.GetLastLogEntryIndex())
	rf.thisTermMatchedLeader = true
	return true
}

func (rf *Raft) pushToReplyCh() {
	for !rf.killed() {
		select {
		case <-rf.closeCh:
			return
		//golang中的select没法保证执行的优先级，也就是说有可能applyTransCh中还有东西，但是
		//本go程直接return了。实际上这也符合正常情况，例如一个raft peer已经提交了一个command，
		//但是还没来得及apply到状态机中，就狗带了。这种情况raft回复后会再次reply。所以不需要处理
		case msg := <-rf.applyTransCh:
			rf.applyCh <- msg
		}
	}
}
