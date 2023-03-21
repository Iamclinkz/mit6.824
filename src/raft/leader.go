package raft

var _ StateHandler = (*LeaderStateHandler)(nil)

type LeaderStateHandler struct {
	StateHandlerBase
}

func (rf LeaderStateHandler) OnInstallSnapshotRequestReply(msg *InstallSnapshotReplyMsg) {
	if msg.reply.Term != rf.getTerm() {
		rf.setTerm(msg.reply.Term)
		rf.setState(Follower)
	}

	if msg.args.LastIncludeIndex <= rf.nextIndex[msg.serverID] {
		return
	}

	rf.nextIndex[msg.serverID] = msg.args.LastIncludeIndex
	rf.log(dSnap, "revive success InstallSnapshot msg from S%v, update nextIndex:%v",
		msg.serverID, msg.args.LastIncludeIndex)
}

func (rf LeaderStateHandler) OnClientCmdArrive(commandWithNotify *CommandWithNotifyCh) {
	pos := rf.leaderAddCommand(commandWithNotify.command)
	commandWithNotify.finishWithOK(rf.getTerm(), pos)
}

func (rf LeaderStateHandler) OnAppendEntriesReply(msg *AppendEntriesReplyMsg) {
	myTerm := rf.getTerm()
	peerID := msg.serverID

	if msg.reply.Term < myTerm || msg.args.PrevLogIndex != rf.nextIndex[msg.serverID]-1 {
		//测试了10000次才发现的bug，如果我从leader->follower->leader，并且收到了之前的rpc，那么可能有问题
		rf.log(dWarn, "receive S%v AppendEntryReply, PrevLogIndex:%v != next-1:%v",
			peerID, msg.args.PrevLogIndex, rf.nextIndex[msg.serverID]-1)
		return
	}

	//不成功，且对方term较大，自己变为follower
	if msg.reply.Term > myTerm {
		rf.log(dWarn, "receive S%v AppendEntryReply, term:%v bigger than mine, change to follower",
			peerID, msg.reply.Term)
		rf.setTerm(msg.reply.Term)
		rf.setState(Follower)
		return
	}

	if msg.reply.Success {
		//如果成功，那么设置matchIndex和nextIndex

		nextIndexFromReply := msg.args.PrevLogIndex + len(msg.args.Entries) + 1
		nextMatchFromReply := nextIndexFromReply - 1

		//这里rpc可能乱序，所以需要保证一手小的不能覆盖大的
		//如果msg中的条目都匹配成功了，但是以前就匹配过了，那么可能是rpc乱序了，也可能是本次没有增加新的条目，总之我们不做处理
		if nextIndexFromReply <= rf.nextIndex[peerID] || nextMatchFromReply <= rf.matchIndex[peerID] {
			return
		}

		if nextIndexFromReply != rf.nextIndex[peerID] {
			rf.log(dLeader, "S%v success append log %v to %v", peerID, rf.nextIndex[peerID], nextIndexFromReply-1)
			rf.nextIndex[peerID] = nextIndexFromReply
		}

		if nextMatchFromReply != rf.matchIndex[peerID] {
			rf.log(dLeader, "S%v match log %v to %v", peerID, rf.matchIndex[peerID]+1, nextMatchFromReply)
			rf.matchIndex[peerID] = nextMatchFromReply
		}

		//rf.log(dLeader,"receive success reply from S%v, match:%v, next:%v",
		//	peerID,rf.matchIndex[peerID],rf.nextIndex[peerID])
		return
	}

	//不成功，但是我们的term起码和对方一样大，如果选举过程没啥问题，说明本次发的没有对方希望的日志（没有匹配成功）
	//回退一下nextIndex

	//rf.nextIndex[peerID]是没匹配上的日志，回退到没匹配上的日志的上一个term的第一条进行发送
	//最极端的情况，当回退到 rf.nextIndex[peerID] == rf.logEntries.LastIncludeIndex 时，说明我们当前的
	//rf.logEntries.logs已经无法让follower匹配了，这种情况下，我们设置rf.nextIndex[peerID]为
	//rf.logEntries.LastIncludeIndex 这样下次心跳的时候，发送安装快照rpc，而不是增加日志rpc。
	lastIncludeIndex := rf.logEntries.LastIncludeIndex
	unMatchIdx := rf.nextIndex[peerID]

	unMatchEntry := rf.logEntries.Get(unMatchIdx)
	if rf.nextIndex[peerID] <= lastIncludeIndex+1 || unMatchEntry == nil ||
		rf.logEntries.GetLastIncludeTerm() == unMatchEntry.Term {
		//如果当前已经无法再回退了（已经退到snapshot的最后一条），再或者匹配失败的term == LastIncludeTerm，
		//我们直接放弃匹配，设置 rf.nextIndex[peerID] 为snapshot的最后一条，下个心跳发送安装rpc
		rf.nextIndex[peerID] = lastIncludeIndex
		rf.log(dLeader, "receive fail reply from S%v, need to send install snapshot!", peerID)
		return
	}

	start := unMatchIdx - 1
	term := rf.logEntries.Get(start).Term

	for rf.logEntries.Get(start).Term == term {
		start--
	}

	if rf.logEntries.GetLastIncludeTerm() == rf.logEntries.Get(start).Term {
		//如果当前已经无法再回退了（已经退到snapshot的最后一条），再或者匹配失败的term == LastIncludeTerm，
		//我们直接放弃匹配，设置 rf.nextIndex[peerID] 为snapshot的最后一条，下个心跳发送安装rpc
		rf.nextIndex[peerID] = lastIncludeIndex
		rf.log(dLeader, "receive fail reply from S%v, need to send install snapshot!", peerID)
		return
	}

	term = rf.logEntries.Get(start).Term
	for rf.logEntries.Get(start).Term == term && start != lastIncludeIndex {
		start--
	}

	rf.nextIndex[peerID] = start + 1
	rf.log(dLeader, "receive fail reply from S%v, match:%v, next:%v",
		peerID, rf.matchIndex[peerID], rf.nextIndex[peerID])
}

func (rf LeaderStateHandler) OnCandidateOverTimeTick() {
	return
}

func (rf LeaderStateHandler) OnQuitState() {
	return
}

func (rf LeaderStateHandler) OnEnterState() {
	//重置nextIndex数组和matchIndex数组
	l := rf.logEntries.Len()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = l
			rf.matchIndex[i] = 0
		}
	}
	rf.minLogNextIndex = l

	//自己的nextIndex为len(rf.Logs)，表示不需要发送
	rf.nextIndex[rf.me] = l
	//自己的matchIndex设置成len(rf.Logs) - 1，表示rf.logs中所有的日志都已经和自己的相同
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
		return rf.CurrentStateHandler.HandleAppendEntries(args, reply)
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
	if rf.updateCommitIndex() {
		rf.applyLog(rf.getLastCommitIdx())
	}

	rf.log(dTrace, "send heartbeat to other server...")
	rf.sendHeartBeat()
}

func (rf LeaderStateHandler) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	myTerm := rf.getTerm()

	if myTerm < args.Term {
		rf.setState(Follower)
		rf.setTerm(args.Term)
		reply.Term = args.Term
		return rf.FollowerStateHandlerInst.HandleRequestVote(args, reply)
	}

	reply.VoteGranted = false
	reply.Term = args.Term
	return nil
}

func (rf LeaderStateHandler) HandleNeedElection() {
	return
}
