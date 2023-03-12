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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//todo 暂时设定只有主协程可以写，其他的只可以读，防止多个协程之间写出错（例如由大变小）
	currentTerm int32 //当前的轮次

	//todo 这个字段暂且不加锁了，当前之后主协程访问，之后看情况加锁
	votedFor int //本轮投给谁了，注意这个字段需要和leader保持相同

	//保护下面的几个字段
	leaderMu      sync.Mutex
	currentLeader int

	//log 有关
	logMu sync.Mutex  //下面几个字段日志相关的字段的锁
	logs  []*LogEntry //初始为1
	//下面两个字段的关系可以见 https://www.zhihu.com/question/61726492/answer/190736554
	//commitIndex 是本raft感知到(commit)的最后一条log entry的index
	//lastApplied 是本raft所在的状态机器最后一条应用的index
	commitIndex int //已经提交的最后一条log entry的index
	commitTerm  int //已经提交的最后一条log entry的term

	lastApplied int //已经被state machine应用的最后一条log entry的index

	//leader使用
	nextIndex  []int //index为peer的index，value为下一个应该发送的log entry的下标（开始为leader的last log + 1）
	matchIndex []int //index为peer的index，value为该peer已经复制(replicated)的log entry的下标（开始为0）

	//日志有关
	//log func(topic logTopic,format string, a ...interface{})

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
	lastHeartBeatTime    int64                    //上次heartBeat的时间
	needHeartBeat        chan struct{}            //需要发送心跳
	appendEntriesReqCh   chan *rpcChMsg           //附加日志请求chan
	appendEntriesReplyCh chan *AppendEntriesReply //附加日志回复chan

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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setHeartBeat() {
	now := time.Now().UnixMicro()
	atomic.StoreInt64(&rf.lastHeartBeatTime, now)
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

	rf.needElectionCh = make(chan struct{})
	rf.requestVoteReqCh = make(chan *rpcChMsg, 100)
	rf.logs = make([]*LogEntry, 0)
	// Your initialization code here (2A, 2B, 2C).

	rand.Seed(makeSeed())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//rf.stopCandidateCh = make(chan struct{},1)

	rf.leastVoterNum = len(peers)/2 + 1

	rf.requestVoteReplyCh = make(chan *RequestVoteReply, len(rf.peers))

	// start startFollowerHeartBeatCheckTicker goroutine to start elections

	rf.wg = &sync.WaitGroup{}

	rf.goF(rf.startFollowerHeartBeatCheckTicker, "FollowerHeartBeatCheckTicker")
	rf.goF(rf.startLeaderHeartBeatTicker, "LeaderHeartBeatTicker")
	rf.goF(rf.startMainLoop, "Main Loop")

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

		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
}

//appendLog commit一条日志，index为该日志的index。注意日志中的Term字段不应该为0
func (rf *Raft) appendLog(entry *LogEntry, index int) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()

	if entry == nil || entry.Term == 0 {
		panic("error append log")
	}

	rf.logs = append(rf.logs, entry)
	rf.commitTerm = entry.Term
	rf.commitIndex = index
}
