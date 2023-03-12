package raft

import "time"

var _ StateHandler = (*CandidateStateHandler)(nil)

type CandidateStateHandler struct {
	StateHandlerBase
}

func (rf CandidateStateHandler) OnAppendEntriesReply(reply *AppendEntriesReply) {
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

//清空所有的竞选有关的chan
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
	rf.log(dVote, "receive vote from server:%v", reply.ServerID)
	rf.voter[reply.ServerID] = struct{}{}
	if len(rf.voter) >= rf.leastVoterNum {
		rf.log(dVote, "I am the new leader")
	}
	rf.setState(Leader)
}

func (rf CandidateStateHandler) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	myTerm := rf.getTerm()

	//如果自己的term比较小/收到了新的领导人的心跳，那么从candidate变回follower
	if myTerm <= args.Term {
		rf.setTerm(args.Term)

		rf.setState(Follower)
		//select {
		//case rf.stopCandidateCh <- struct{}{}:
		//default:
		//}

		reply.Success = true
		reply.Term = args.Term
		return nil
	}

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

		//select {
		//case rf.stopCandidateCh <- struct{}{}:
		//default:
		//}
		rf.setState(Follower)
		reply.VoteGranted = true
		reply.Term = args.Term
		return nil
	}

	//自己的term和请求投票者相同 or 自己的较大，均不投票给对方
	reply.VoteGranted = false
	reply.Term = myTerm
	return nil
}

//HandleNeedElection 开始选举，向所有的除了自己的server发送请求，然后去主线程等待回复
func (rf CandidateStateHandler) HandleNeedElection() {
	rf.incTerm()
	myTerm := rf.getTerm()

	req := &RequestVoteArgs{
		Term:        myTerm,
		CandidateId: rf.me,
	}

	rf.voter[rf.me] = struct{}{}

	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go rf.sendRequestVote(serverID, req, &RequestVoteReply{})
		}
	}
}