package raft

var _ StateHandler = (*FollowerStateHandler)(nil)

type FollowerStateHandler struct {
	StateHandlerBase
}

func (rf FollowerStateHandler) OnInstallSnapshotRequestReply(msg *InstallSnapshotReplyMsg) {
	return
}

func (rf FollowerStateHandler) HandleInstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotRequestReply) error {
	myTerm := rf.getTerm()
	reply.Term = myTerm
	if args.Term != myTerm {
		if myTerm > args.Term {
			return nil
		}

		if myTerm < args.Term {
			rf.setTerm(args.Term)
		}
	}

	if args.LastIncludeIndex <= rf.logEntries.GetLastLogEntryIndex() {
		//如果已经安装了，那么不需要重复安装快照
		return nil
	}

	rf.applyTransCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	return nil
}

func (rf FollowerStateHandler) OnClientCmdArrive(commandWithNotify *CommandWithNotifyCh) {
	commandWithNotify.finishWithError()
}

func (rf FollowerStateHandler) OnAppendEntriesReply(reply *AppendEntriesReplyMsg) {
	return
}

func (rf FollowerStateHandler) OnCandidateOverTimeTick() {
	return
}

func (rf FollowerStateHandler) OnQuitState() {
	return
}

func (rf FollowerStateHandler) OnEnterState() {
	rf.reSetHeartBeat()
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
		//这里实际上不需要管leader是不是已经换过一轮了。因为raft论文中有提到，如果两个raft peer在相同的index处，有termID相同的
		//两条日志，那么这两个日志的command也一定相同。并且我们只是根据：
		// rf.Logs[args.PrevLogIndex].Term == args.PrevLogTerm
		//这一条匹配规则匹配，也就是说无论是新leader还是旧leader，根本不需要进行区分
		rf.setTerm(args.Term)
	}

	rf.log(dLog2, "receive heartBeat from S%v, lastIdx:%v, lastTerm:%v, entriesLen:%v",
		args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	rf.reSetHeartBeat()

	reply.Success = rf.doAppendEntry(args)
	reply.Term = args.Term

	if reply.Success && rf.getLastCommitIdx() < args.LeaderCommit {
		//如果leader更新了commitIndex，那么应用一下
		//这里发现了一个bug，就是可能leader的commit index还不如我们（因为选举的时候看的是最后一条日志，而不是commitIndex）
		//那么不用管这种情况
		rf.setLastCommitIdx(args.LeaderCommit)
		rf.applyLog(args.LeaderCommit)
	}
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
		//注意setTerm会将VoteFor清除，但是本来新的一个Term就是要清除，所以这里没啥问题
		rf.setTerm(args.Term)
	}

	//lab2B，论文5.4.1中的选举限制
	if !rf.logEntriesNewerThanMe(args.LastLogTerm, args.LastLogIndex, args.CandidateId) {
		//如果对方不如我们新
		reply.VoteGranted = false
		reply.Term = args.Term
		return nil
	}

	rf.reSetHeartBeat()
	reply.VoteGranted = true
	reply.Term = args.Term
	rf.setVotedFor(args.CandidateId)
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
