package raft

import "time"

var _ StateHandler = (*CandidateStateHandler)(nil)

type CandidateStateHandler struct {
	StateHandlerBase
}

func (rf CandidateStateHandler) OnInstallSnapshotRequestReply(msg *InstallSnapshotReplyMsg) {
	return
}

func (rf CandidateStateHandler) OnClientCmdArrive(commandWithNotify *CommandWithNotifyCh) {
	commandWithNotify.finishWithError()
}

func (rf CandidateStateHandler) OnAppendEntriesReply(reply *AppendEntriesReplyMsg) {
	return
}

func (rf CandidateStateHandler) OnCandidateOverTimeTick() {
	//通过自己和自己切换，重置各种内容
	rf.setState(Candidate)
	rf.CurrentStateHandler.HandleNeedElection()
}

func (rf CandidateStateHandler) OnQuitState() {
	rf.clearCandidateCh()
}

// 清空所有的竞选有关的chan
func (rf CandidateStateHandler) clearCandidateCh() {
	for {
		select {
		//case <- rf.stopCandidateCh:
		case <-rf.requestVoteReplyCh:
		default:
			return
		}
	}
}

func (rf CandidateStateHandler) OnEnterState() {
	rf.clearCandidateCh()
	rf.voter = make(map[int]struct{})
	rf.candidateOverTimeTick = time.After(getRandCandidateOverTimeTickDuration())
}

func (rf CandidateStateHandler) OnRequestVoteReply(reply *RequestVoteReply) {
	myTerm := rf.getTerm()
	if !reply.VoteGranted {
		//如果不成功，并且对方的term比我们当前的高，那么直接退出
		if reply.Term > myTerm {
			rf.setTerm(reply.Term)
			rf.setState(Follower)
		}

		//如果term相同，但是不接受，可能是上一轮我们的请求发给了对方，也可能是自己和对方同时开始选举了，总之失败
		return
	}

	if reply.VoteGranted {
		//如果成功
		if reply.Term > myTerm {
			rf.log(dError, "program fault! RequestVoteReply's term should not greater than my current term!")
			panic("")
		}

		if reply.Term < myTerm {
			//可能是本term收到了上一个term的成功回复，那么不算数
			return
		}
	}

	//对方成功给我们投票
	rf.voter[reply.ServerID] = struct{}{}
	voters := make([]int, len(rf.voter))
	idx := 0
	for serverID, _ := range rf.voter {
		voters[idx] = serverID
		idx++
	}
	rf.log(dVote, "receive vote from S%v, current voters:%v",
		reply.ServerID, voters)
	if len(rf.voter) >= rf.leastVoterNum {
		rf.log(dLeader, "I am the new leader")
		rf.setState(Leader)
	}
}

func (rf CandidateStateHandler) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	myTerm := rf.getTerm()

	//如果自己的term比较小/收到了新的领导人的心跳，那么从candidate变回follower
	//注：如果发来心跳的leader的term比较大，那么这个不用说，肯定是认对方
	//如果发来心跳的leader的term和我们相同，那么对方在本term已经拿到了超过半数的选票，自己肯定赢不了，所以认对方。
	//因为对方肯定本term拿到了足够多的选票了，所以不需要再验证log entry是否比我新了
	if myTerm <= args.Term {
		rf.setTerm(args.Term)
		rf.setState(Follower)
		return rf.CurrentStateHandler.HandleAppendEntries(args, reply)
	}

	//如果我的比较大，不承认对方
	reply.Success = false
	reply.Term = myTerm
	return nil
}

func (rf CandidateStateHandler) LeaderHeartBeat() {
	return
}

func (rf CandidateStateHandler) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	myTerm := rf.getTerm()

	//如果自己的term较小，那么放弃竞选
	if myTerm < args.Term {
		rf.setTerm(args.Term)
		rf.setState(Follower)
		return rf.FollowerStateHandlerInst.HandleRequestVote(args, reply)
	}

	//自己的term和请求投票者相同 or 自己的较大，均不投票给对方
	reply.VoteGranted = false
	reply.Term = myTerm
	return nil
}

// HandleNeedElection 开始选举，向所有的除了自己的server发送请求，然后去主线程等待回复
func (rf CandidateStateHandler) HandleNeedElection() {
	rf.incTerm()
	myTerm := rf.getTerm()

	req := &RequestVoteArgs{
		Term:         myTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogEntryIndex(),
		LastLogTerm:  rf.getLastLogEntryTerm(),
	}

	rf.voter[rf.me] = struct{}{}
	rf.setVotedFor(rf.me)

	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go rf.sendRequestVote(serverID, req, &RequestVoteReply{})
		}
	}
}
