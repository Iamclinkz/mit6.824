package raft

type StateHandler interface {
	//HandleRequestVote 处理来自其他server的请求投票rpc
	HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error

	//HandleAppendEntries 处理来自其他server的请求追加日志条目rpc
	HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error

	//HandleInstallSnapshot 处理来自其他server的安装快照rpc
	HandleInstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotRequestReply) error

	//HandleNeedElection 处理心跳超时引起的自己需要参与竞选事件
	HandleNeedElection()

	//LeaderHeartBeat 处理自己是leader，需要给其他server发送心跳事件
	LeaderHeartBeat()

	//OnRequestVoteRet 请求投票rpc的返回
	OnRequestVoteReply(reply *RequestVoteReply)

	//OnQuitState 本state退出的时候，执行的回调，只能进行数据的初始化/通知其他进程，不可以在这里面切换状态！
	OnQuitState()
	//OnEnterState 本state进入的时候，执行的回调
	OnEnterState()

	//OnCandidateOverTimeTick 当选举时钟超时
	OnCandidateOverTimeTick()

	OnAppendEntriesReply(reply *AppendEntriesReplyMsg)

	OnClientCmdArrive(commandWithNotify *CommandWithNotifyCh)

	//OnSnapshot 处理来自上层service的将快照替换日志的请求
	HandleSnapshot(req *SnapshotRequest)
}

//StateHandlerBase 内部持有了三种handler共用的结构
type StateHandlerBase struct {
	*Raft
}

func (rf *Raft) initHandler() {
	rf.CandidateStateHandlerInst.Raft = rf
	rf.LeaderStateHandlerInst.Raft = rf
	rf.FollowerStateHandlerInst.Raft = rf
	rf.CurrentStateHandler = rf.FollowerStateHandlerInst
}

func (rf StateHandlerBase) HandleSnapshot(req *SnapshotRequest) {
	myLastLogEntryIndex := rf.getLastLogEntryIndex()
	if req.idx <= myLastLogEntryIndex {
		//如果以前已经压缩过了，那么直接返回
		return
	}

	if req.idx >= myLastLogEntryIndex {
		if entry := rf.logEntries.Get(req.idx); entry == nil {
			rf.log(dError, "should no Snapshot logs which have not been appended! current lastIdx:%v, req.idx:%v",
				myLastLogEntryIndex, req.idx)
			panic("")
		} else {
			rf.snapshot = req.snapshot
			rf.logEntries.Reinit(entry.Term, myLastLogEntryIndex)
			rf.log(dSnap, "new snapshot: [%v,%v]", myLastLogEntryIndex, req.idx)
		}
	}
}
