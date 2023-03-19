package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// 如果一个log entry被commit了（某个日志已经被大多数节点保存），那么每个节点都应该给apply到自己的状态机上
// server发一个ApplyMsg，表示应该apply这个日志条目
// 注意，每个raft日志里面是包含了一个对状态机操作的指令的
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool        //是否应该被apply
	Command      interface{} //被apply的指令本身
	CommandIndex int         //指令的index

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type CommandWithNotifyCh struct {
	command  interface{}
	notifyCh chan *CommandNotify //如果成功放到leader的logs中，那么返回index，否则返回-1
}

type CommandNotify struct {
	ok   bool
	term int
	idx  int
}

//pushCommand 副go程接受的来自客户端的command，扔给主go程处理
func (rf *Raft) pushCommand(command interface{}) chan *CommandNotify {
	commandWithNotifyCh := &CommandWithNotifyCh{
		command:  command,
		notifyCh: make(chan *CommandNotify, 1),
	}
	rf.commandCh <- commandWithNotifyCh

	return commandWithNotifyCh.notifyCh
}

//finishWithError 主go程出错（例如当前状态不对），给客户端返回错误
func (c *CommandWithNotifyCh) finishWithError() {
	c.notifyCh <- &CommandNotify{
		term: -1,
		idx:  -1,
		ok:   false,
	}
}

func (c *CommandWithNotifyCh) finishWithOK(term, idx int) {
	c.notifyCh <- &CommandNotify{
		term: term,
		idx:  idx,
		ok:   true,
	}
}

type LogEntry struct {
	Command interface{}
	//本LogEntry中的Command字段，被leader接受的时候，leader的term
	Term int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的当前Term
	CandidateId  int //候选人的ID
	LastLogIndex int //候选人的最后一条日志条目的index
	LastLogTerm  int //候选人最后一条日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //回复者的任期号
	VoteGranted bool //是否支持候选人
	ServerID    int  //只用在内部主线程回调，不用在rpc发送中，懒得再封装一个结构了
	Error       string
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.log(dTrace, "send RequestVote to S%v", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok || reply.Error != "" {
		return
	}

	reply.ServerID = server
	if reply.VoteGranted {
		rf.log(dTrace, "S%v voted:√", server)
	} else {
		rf.log(dTrace, "S%v voted:×", server)
	}

	rf.requestVoteReplyCh <- reply
	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.Error = killedError.Error()
		rf.log(dWarn, "I was killed, but got RequestVote from S%v", args.CandidateId)
		return
	}

	// Your code here (2A, 2B).
	rf.log(dTrace, "got RequestVote rpc from S%v: %+v", args.CandidateId, *args)
	ch := pushRpcChan(args, reply, rf.requestVoteReqCh)
	err := <-ch
	if err != nil {
		rf.log(dWarn, "finish RequestVote rpc from server, error:%v", err)
		reply.Error = killedError.Error()
		return
	}
	rf.log(dTrace, "finish RequestVote rpc from server:%v, reply: %+v", args.CandidateId, *reply)
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int         //leader当前term
	LeaderId     int         //leader的id
	PrevLogIndex int         //紧邻第一个日志条目（即Entries[0]）之前的那个日志条目的索引，指的是rf.logs的下标
	PrevLogTerm  int         //紧邻第一个日志条目（即Entries[0]）之前的那个日志条目的任期，指的是rf.logs的下标
	Entries      []*LogEntry //需要被保存的,新的日志条目。规定如果不需要发任何东西的话，这个字段为nil
	LeaderCommit int         //领导人的已知已提交的最高的日志条目的索引，注意指的是rf.logs的下标
}

type AppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term    int    //自己的term
	Success bool   //是否成功
	Error   string //是否失败
}

type AppendEntriesReplyMsg struct {
	args     *AppendEntriesArgs
	reply    *AppendEntriesReply
	serverID int
}

//sendHeartBeat 给所有的server发送带有日志目录的心跳，不阻塞
func (rf *Raft) sendHeartBeat() {
	//[ rf.minLogNextIndex , len(rf.logs) ) 的内容需要被拷贝缓存一手，再发送。防止主go程在发送期间对logs增删

	logs := make([]*LogEntry, len(rf.logs))
	copy(logs, rf.logs)

	//if rf.minLogNextIndex == len(rf.logs){
	//	//如果当前没有要发送的，发送简单的心跳
	//	req := &AppendEntriesArgs{
	//		Term:     rf.getTerm(),
	//		LeaderId: rf.me,
	//		LeaderCommit: rf.commitIndex,
	//	}
	//
	//	for serverID, _ := range rf.peers {
	//		if serverID != rf.me {
	//			go rf.sendAppendEntries(serverID, req, &AppendEntriesReply{})
	//		}
	//	}
	//	return
	//}

	myTerm := rf.getTerm()
	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			req := &AppendEntriesArgs{
				Term:         myTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.getLastCommitIdx(),
				//因为有了哨兵，所以这里可以不用判断当前有无日志，即时没有日志，这里也会发送
				//PrevLogIndex = 0
				//PrevLogTerm = rf.logs[0].Term (即-1)
				//Entries = []
				PrevLogIndex: rf.nextIndex[serverID] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[serverID]-1].Term,
				Entries:      logs[rf.nextIndex[serverID]:],
			}
			go rf.sendAppendEntries(serverID, req, &AppendEntriesReply{})
		}
	}
	return
}

func (rf *Raft) genAppendEntriesArgs(serverID int) *AppendEntriesArgs {
	//todo 仔细检查一下
	var entries []*LogEntry
	prevLogIndex := rf.nextIndex[serverID] - 1
	prevLogTerm := -1

	if prevLogIndex != 0 {
		prevLogTerm = rf.logs[prevLogIndex].Term
		entries = rf.logs[rf.nextIndex[serverID]:]
	}

	return &AppendEntriesArgs{
		Term:         rf.getTerm(),
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.getLastCommitIdx(),
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok || reply.Error != "" {
		return
	}

	msg := &AppendEntriesReplyMsg{
		args:     args,
		reply:    reply,
		serverID: server,
	}

	rf.appendEntriesReplyCh <- msg
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Error = killedError.Error()
		rf.log(dWarn, "I was killed, but got AppendEntries from S%v", args.LeaderId)
		return
	}

	ch := pushRpcChan(args, reply, rf.appendEntriesReqCh)
	err := <-ch
	if err != nil {
		rf.log(dWarn, "finish append entry with error:%v", err)
		reply.Error = err.Error()
	}
}

func (rf *Raft) GenAppendEntriesArgs(serverID int) *AppendEntriesArgs {
	return nil
}
