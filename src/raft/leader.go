package raft

var _ StateHandler = (*LeaderStateHandler)(nil)

type LeaderStateHandler struct {
	StateHandlerBase
}

func (rf LeaderStateHandler) OnClientCmdArrive(commandWithNotify *CommandWithNotifyCh) {
	pos := rf.leaderAddCommand(commandWithNotify.command)
	commandWithNotify.finishWithOK(rf.getTerm(),pos)
}

func (rf LeaderStateHandler) OnAppendEntriesReply(msg *AppendEntriesReplyMsg) {
	myTerm := rf.getTerm()

	if msg.reply.Term < myTerm{
		//测试了10000次才发现的bug，如果我从leader->follower->leader，并且收到了之前的rpc，那么可能有问题
		return
	}

	peerID := msg.serverID
	if msg.reply.Success {
		//如果成功，那么设置matchIndex和nextIndex
		rf.matchIndex[peerID] = rf.nextIndex[peerID] + len(msg.args.Entries) - 1
		rf.nextIndex[peerID] = rf.nextIndex[peerID] + len(msg.args.Entries)
		rf.log(dLeader,"receive success reply from S%v, match:%v, next:%v",
			peerID,rf.matchIndex[peerID],rf.nextIndex[peerID])
		return
	}

	//不成功，且对方term较大，自己变为follower
	if msg.reply.Term > myTerm{
		rf.setTerm(msg.reply.Term)
		rf.setState(Follower)
		return
	}

	//不成功，但是我们的term起码和对方一样大，如果选举过程没啥问题，说明本次发的没有对方希望的日志（没有匹配成功），回退一下nextIndex
	//todo 待优化
	rf.nextIndex[peerID]--

	if rf.nextIndex[peerID] == 0{
		//如果回退到0的位置，说明0也匹配不上。和预期不符，错误
		rf.log(dError,"there is no match log between me and S%v, AppendEntriesArg:%+v,Reply:%+v",
			peerID,msg.args,msg.reply)
		panic("")
	}
	if rf.minLogNextIndex < rf.nextIndex[peerID]{
		rf.minLogNextIndex = rf.nextIndex[peerID]
	}

	rf.log(dLeader,"receive fail reply from S%v, match:%v, next:%v",
		peerID,rf.matchIndex[peerID],rf.nextIndex[peerID])
}

func (rf LeaderStateHandler) OnCandidateOverTimeTick() {
	return
}

func (rf LeaderStateHandler) OnQuitState() {
	return
}

func (rf LeaderStateHandler) OnEnterState() {
	//重置nextIndex数组和matchIndex数组
	l := len(rf.logs)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me{
			rf.nextIndex[i] = l
			rf.matchIndex[i] = 0
		}
	}
	rf.minLogNextIndex = l

	//自己的nextIndex为len(rf.logs)，表示不需要发送
	rf.nextIndex[rf.me] = l
	//自己的matchIndex设置成len(rf.logs) - 1，表示rf.logs中所有的日志都已经和自己的相同
	rf.matchIndex[rf.me] = l - 1

	//当新成为leader时，应该直接跟其他server发送心跳
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
		return rf.CurrentStateHandler.HandleAppendEntries(args,reply)
	}

	if args.Term == myTerm {
		rf.log(dError, "this term has two leaders! leader1:%v, leader2:%v", rf.me, args.LeaderId)
		panic("")
	}

	//如果发过来的请求中的日志index和日志term和现在的不匹配，那么不接受
	//lastCommitIdx := rf.getLastCommitIdx()
	//lastCommitTerm := rf.getLastCommitTerm()
	return nil
}

func (rf LeaderStateHandler) LeaderHeartBeat() {
	if rf.updateCommitIndex(){
		rf.applyLog(rf.commitIndex)
	}

	rf.log(dLeader, "send heartbeat to other server...")
	rf.sendHeartBeat()
}

func (rf LeaderStateHandler) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	myTerm := rf.getTerm()

	if myTerm < args.Term {
		rf.setState(Follower)
		rf.setTerm(args.Term)
		reply.Term = args.Term
		return rf.FollowerStateHandlerInst.HandleRequestVote(args,reply)
	}

	reply.VoteGranted = false
	reply.Term = args.Term
	return nil
}

func (rf LeaderStateHandler) HandleNeedElection() {
	return
}
