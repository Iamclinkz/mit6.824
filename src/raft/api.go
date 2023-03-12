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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.log(dVote, "send RequestVote to S%v", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	reply.ServerID = server
	rf.log(dVote, "get RequestVote response from S%v:%v", server, *reply)
	rf.requestVoteReplyCh <- reply
	return true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.log(dVote, "got RequestVote rpc from server:%v: ", args.CandidateId, *args)
	ch := pushRpcChan(args, reply, rf.requestVoteReqCh)
	err := <-ch
	if err != nil {
		reply.VoteGranted = false
		reply.Term, _ = rf.GetState()
	}
	rf.log(dVote, "finish RequestVote rpc from server:%v, reply: %v", args.CandidateId, *reply)
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int         //leader当前term
	LeaderId     int         //leader的id
	PrevLogIndex int         //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int         //紧邻新日志条目之前的那个日志条目的任期
	Entries      []*LogEntry //需要被保存的,新的日志条目
	LeaderCommit int         //领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term     int  //自己的term
	Success  bool //是否成功
	ServerID int
}

//sendHeartBeat 给所有的server发送心跳，不阻塞
func (rf *Raft) sendHeartBeat() {
	req := &AppendEntriesArgs{
		Term:     rf.getTerm(),
		LeaderId: rf.me,
	}

	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go rf.sendAppendEntries(serverID, req, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}

	reply.ServerID = server
	rf.appendEntriesReplyCh <- reply
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ch := pushRpcChan(args, reply, rf.appendEntriesReqCh)
	err := <-ch
	if err != nil {
		reply.Success = false
		reply.Term, _ = rf.GetState()
	}
}
