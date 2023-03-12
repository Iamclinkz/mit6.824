package raft

var _ StateHandler = (*FollowerStateHandler)(nil)

type FollowerStateHandler struct {
	StateHandlerBase
}

func (rf FollowerStateHandler) OnAppendEntriesReply(reply *AppendEntriesReply) {
	return
}

func (rf FollowerStateHandler) OnCandidateOverTimeTick() {
	return
}

func (rf FollowerStateHandler) OnQuitState() {
	return
}

func (rf FollowerStateHandler) OnEnterState() {
	return
}

func (rf FollowerStateHandler) OnRequestVoteReply(reply *RequestVoteReply) {
	//已经不是竞选人了，不做额外的处理
	return
}

func (rf FollowerStateHandler) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	myTerm := rf.getTerm()

	//如果领导人的任期小于自己当前的任期，那么不接受
	if args.Term < myTerm {
		reply.Success = false
		reply.Term = myTerm
		return nil
	}

	if args.Term > myTerm {
		rf.setTerm(args.Term)
	}

	//如果是心跳
	if args.Entries == nil {
		rf.setHeartBeat()
	}

	reply.Success = true
	reply.Term = args.Term

	return nil
}

func (rf FollowerStateHandler) LeaderHeartBeat() {
	return
}

func (rf FollowerStateHandler) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	myTerm := rf.getTerm()

	//如果竞选者的term还不如我的term，那么拒绝
	if myTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = myTerm
		return nil
	}

	//如果我和竞选者的term一样大，那么看看有没有voted for，
	//如果当前轮次已经投票了，且不是竞选者，那么不投竞选者
	if myTerm == args.Term && !rf.noVoted() && rf.getVotedFor() != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = myTerm
		return nil
	}

	//如果竞选者的term较大/当前轮次没有投票/当前轮次投的就是该竞选者，那么设置我的term跟他一样，并且给他投票
	if myTerm < args.Term {
		rf.setTerm(args.Term)
	}

	reply.VoteGranted = true
	reply.Term = args.Term
	rf.setVoted(args.CandidateId)
	//myLastLogIdx := rf.getLastCommitIdx()
	//myLastLogTerm := rf.getLastCommitTerm()

	//检查日志是否起码和自己一样新
	//if myLastLogIdx > args.LastLogIndex || myLastLogTerm > args.LastLogTerm{
	//	reply.VoteGranted = false
	//}
	return nil
}

func (rf FollowerStateHandler) HandleNeedElection() {
	rf.log(dTimer, "find leader heartBeat over time, start election!")
	rf.setState(Candidate)
	rf.CurrentStateHandler.HandleNeedElection()
}
