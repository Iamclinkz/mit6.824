package raft

var _ StateHandler = (*LeaderStateHandler)(nil)

type LeaderStateHandler struct {
	StateHandlerBase
}

func (rf LeaderStateHandler) OnAppendEntriesReply(reply *AppendEntriesReply) {
	myTerm := rf.getTerm()
	if reply.Success || reply.Term <= myTerm {
		return
	}

	rf.setTerm(reply.Term)
	rf.setState(Follower)
}

func (rf LeaderStateHandler) OnCandidateOverTimeTick() {
	return
}

func (rf LeaderStateHandler) OnQuitState() {
	return
}

func (rf LeaderStateHandler) OnEnterState() {
	//当新成为leader时，应该跟其他server发送心跳
	rf.sendHeartBeat()
}

func (rf LeaderStateHandler) OnRequestVoteReply(reply *RequestVoteReply) {
	//这里已经是leader了，不做额外的处理
	return
}

func (rf LeaderStateHandler) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	myTerm := rf.getTerm()

	//如果领导人的任期小于自己当前的任期，那么不接受
	if args.Term < myTerm {
		reply.Success = false
		reply.Term = myTerm
		return nil
	}

	if args.Term > myTerm {
		rf.setState(Follower)
		rf.setTerm(args.Term)
		reply.Term = args.Term
	}

	if args.Term == myTerm {
		rf.log(dLeader, "this term has two leaders! leader1:%v, leader2:%v", rf.me, args.LeaderId)
		panic("I am the leader")
	}

	//如果发过来的请求中的日志index和日志term和现在的不匹配，那么不接受
	//lastCommitIdx := rf.getLastCommitIdx()
	//lastCommitTerm := rf.getLastCommitTerm()
	return nil
}

func (rf LeaderStateHandler) LeaderHeartBeat() {
	rf.log(dLeader, "send heartbeat to other server...")
	rf.sendHeartBeat()
}

func (rf LeaderStateHandler) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	myTerm := rf.getTerm()

	if myTerm < args.Term {
		rf.setState(Follower)
		rf.setTerm(args.Term)
		rf.setVotedFor(args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		return nil
	}

	reply.VoteGranted = false
	reply.Term = args.Term
	return nil
}

func (rf LeaderStateHandler) HandleNeedElection() {
	return
}
