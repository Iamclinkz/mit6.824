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
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// A Go object implementing a single Raft peer.
//
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

	//todo 暂时设定只有主协程可以写，其他的只可以读，防止多个协程之间写出错（例如由大变小）
	currentTerm int32 //当前的轮次

	//todo 这个字段暂且不加锁了，当前之后主协程访问，之后看情况加锁
	votedFor int //本轮投给谁了，注意这个字段需要和leader保持相同

	//保护下面的几个字段
	//leaderMu      sync.Mutex
	currentLeader int

	//log 有关
	//logMu sync.Mutex  //下面几个字段日志相关的字段的锁

	//logs 存放实际的log的切片。注意，使用logs[0]作为哨兵！logs[0].Term = -1
	logs      []*LogEntry               //初始为1
	commandCh chan *CommandWithNotifyCh //只有leader使用，来自客户端的LogEntry
	//下面两个字段的关系可以见 https://www.zhihu.com/question/61726492/answer/190736554
	//commitIndex 是本raft感知到(commit)的最后一条log entry的index
	//lastApplied 是本raft所在的状态机器最后一条应用的index
	commitIndex int //已经提交的最后一条log entry的index，注意不是rf.logs的下标，而是日志的编号！因为有了哨兵所以两者不一样！
	commitTerm  int //已经提交的最后一条log entry的term。注意commit != apply

	lastApplied int //已经被state machine应用的最后一条log entry的index

	//leader使用，一定要注意！！下面的三个字段都表示的是rf.logs的下标，而非log的编号！
	//第n条log被放置在rf.logs[n+1]的位置上！！！
	//index为peer的index，value为下一个应该发送的log entry的下标（开始为leader的last log + 1，也就是len(rf.logs)）
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
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

	if err := e.Encode(rf.logs); err != nil {
		rf.log(dWarn, "failed to persist current logs, len:%v", len(rf.logs))
		return
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.log(dPersist, "successfully persisted information, votedFor:%v, logLen:%v", rf.getVotedFor(), len(rf.logs))
}

//
// restore previously persisted state.
//
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
	var logs []*LogEntry

	if d.Decode(&term) != nil {
		rf.log(dWarn, "failed to load term from persist")
		return
	}

	if d.Decode(&votedFor) != nil {
		rf.log(dWarn, "failed to load votedFor from persist")
		return
	}

	if d.Decode(&logs) != nil {
		rf.log(dWarn, "failed to load logs from persist")
		return
	}

	//因为readPersist只在开始时调用，所以不需要加锁。注意在readPersister中不应该有rf.Persist()的操作！否则可能死锁！
	atomic.StoreInt32(&rf.currentTerm, int32(term))
	rf.votedFor = votedFor
	rf.logs = logs
	rf.log(dPersist, "successful loaded from persist")
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
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
//
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

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.closeCh)
	rf.wg.Wait()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//reSetHeartBeat 将心跳重置，并且清空当前needElectionCh中的请求
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
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
	rf.logs = make([]*LogEntry, 1)
	rf.logs[0] = &LogEntry{
		Command: nil,
		Term:    -1,
	}

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

		case <-rf.closeCh:
			return
		}
	}
}

//leaderAddCommand 只可以在主go程中使用。往log切片中加入一条command
//返回把command加入到的位置
func (rf *Raft) leaderAddCommand(command interface{}) (pos int) {
	entry := &LogEntry{
		Command: command,
		Term:    rf.getTerm(),
	}

	rf.logs = append(rf.logs, entry)
	rf.persist()
	rf.nextIndex[rf.me] = len(rf.logs) + 1
	rf.matchIndex[rf.me] = len(rf.logs)

	pos = len(rf.logs) - 1
	if command == nil {
		rf.log(dWarn, "get nil command, idx:%v", pos)
	}

	rf.log(dLog2, "leader add command, idx:%v", pos)
	return
}

//applyLog 只可以在主go程中使用。将 [ lastApplied + 1, to ] 的日志apply，即传递、应用到上层的状态机
func (rf *Raft) applyLog(to int) {
	if to >= len(rf.logs) {
		//如果希望提交的log的index大于当前logs切片的长度
		rf.log(dError, "try to apply log:%v, which is greater than len(rf.logs):%v", to, len(rf.logs))
		panic("")
	}

	if to <= rf.lastApplied {
		//如果重复提交
		rf.log(dError, "try to apply log:%v, which is greater than or equals to rf.commitIndex:%v", to, rf.getLastCommitIdx())
		panic("")
	}

	//应用到上层的状态机
	for i := rf.lastApplied + 1; i <= to; i++ {
		rf.applyTransCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.log(dClient, "apply log entry, index: %v,term: %v", i, rf.logs[i].Term)
	}

	//rf.log(dClient,"apply log entry: %v to %v",rf.lastApplied + 1,to)
	rf.lastApplied = to
}

//updateCommitIndex 只能在主线程调用，计算，并且更新当前的commitIndex
func (rf *Raft) updateCommitIndex() (updated bool) {
	tmp := make([]int, len(rf.peers))

	//将所有的peer的matchIndex统计（自己的matchIndex为len(rf.logs)-1)，然后找到len(rf.logs)/2 + 1的位置，即commitIndex
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			tmp[i] = rf.matchIndex[i]
		}
	}
	tmp[rf.me] = len(rf.logs) - 1

	QuickSort(tmp, 0, len(tmp)-1)

	//如果不只有一个peer，那么选取len(rf.peers)/2 + 1的位置，作为当前的commitIndex
	oldIdx := rf.getLastCommitIdx()
	newCommitIdx := tmp[len(rf.peers)/2]

	if rf.logs[newCommitIdx].Term != rf.getTerm() {
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

//logEntriesNewerThanMe 另一个peer的logEntries是否比我的新。来自论文5.4.1
//如果完全相同，也返回true。因为论文中都是"起码不比我老"这样的陈述
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

//doAppendEntry 只能在主线程调用，处理来自当前承认的leader的appendEntry指令，返回是否成功
func (rf *Raft) doAppendEntry(args *AppendEntriesArgs) bool {
	//if args.Entries == nil || len(args.Entries) == 0{
	//	//如果是心跳包，那么直接return
	//	return true
	//}

	oldLen := len(rf.logs)
	if args.PrevLogIndex < oldLen && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
		persist := false
		//匹配成功，直接将所有之后的日志删除，并且将发来的日志append到后面去
		//使用哨兵的好处是，即使客户端没有提供任何的日志，这里也能匹配成功
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]
		if oldLen != len(rf.logs) {
			rf.log(dLog2, "delete entry: %v -> %v", len(rf.logs), oldLen-1)
			persist = true
		}
		oldLen = len(rf.logs)

		rf.logs = append(rf.logs, args.Entries...)

		if oldLen != len(rf.logs) {
			rf.log(dLog2, "add entry: %v -> %v", oldLen, len(rf.logs)-1)
			persist = true
		}

		if persist {
			//2C：如果加了或者减了日志，那么需要持久化
			rf.persist()
		}
		return true
	}

	//不匹配，直接return false
	return false
}

func (rf *Raft) pushToReplyCh() {
	for !rf.killed() {
		select {
		case <-rf.closeCh:
			return
		case msg := <-rf.applyTransCh:
			rf.applyCh <- msg
		}
	}
}
