package raft

import "sync/atomic"

type State uint32

const (
	// Follower is the initial state of a Raft node.
	Follower State = iota

	// Candidate is one of the valid states of a Raft node.
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("set error")
	}
}

func (s State) ShortString() string {
	switch s {
	case Follower:
		return "Follow"
	case Candidate:
		return "Candid"
	case Leader:
		return "Leader"
	default:
		panic("set error")
	}
}

func (rf *Raft) GetStateHandler() StateHandler {
	switch rf.getState() {
	case Follower:
		return rf.FollowerStateHandlerInst
	case Candidate:
		return rf.CandidateStateHandlerInst
	case Leader:
		return rf.LeaderStateHandlerInst
	default:
		panic("get handler error")
	}
}

func (rf *Raft) getState() State {
	stateAddr := (*uint32)(&rf.state)
	return State(atomic.LoadUint32(stateAddr))
}

//只有主线程可以set
func (rf *Raft) setState(newState State) {
	rf.CurrentStateHandler.OnQuitState()
	oldState := rf.getState()
	stateAddr := (*uint32)(&rf.state)
	atomic.StoreUint32(stateAddr, uint32(newState))
	rf.CurrentStateHandler = rf.GetStateHandler()
	rf.CurrentStateHandler.OnEnterState()
	rf.log(dLog, "change state %s -> %s", oldState.String(), newState.String())
}

func (rf *Raft) getTerm() int {
	return int(atomic.LoadInt32(&rf.currentTerm))
}

//setTerm 设置当前的term，必须保证前后不同
func (rf *Raft) setTerm(term int) {
	oldTerm := rf.getTerm()

	if oldTerm != term{
		if oldTerm > term {
			rf.log(dError, "old term should not greater than new term!")
			panic("")
		}
		rf.log(dTerm,"term: %v -> %v",oldTerm,term)
		atomic.StoreInt32(&rf.currentTerm, int32(term))
		//2C：这里改变VotedFor的时候，同时也持久化了，所以在外面不再进行持久化
		rf.resetVotedFor()
	}
}

func (rf *Raft) incTerm() {
	rf.setTerm(rf.getTerm() + 1)
}

func (rf *Raft) isLeader() bool {
	return rf.getState() == Leader
}

func (rf *Raft) isCandidate() bool {
	return rf.getState() == Candidate
}

func (rf *Raft) isFollower() bool {
	return rf.getState() == Follower
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.getTerm(), rf.getState() == Leader
}

func (rf *Raft) getLastCommitIdx() int {
	//rf.logMu.Lock()
	//defer rf.logMu.Unlock()

	return rf.commitIndex
}

func (rf *Raft) setLastCommitIdx(idx int) {
	//rf.logMu.Lock()
	//defer rf.logMu.Unlock()

	//简单做个check
	if idx < rf.commitIndex{
		rf.log(dError,"last commit index should not decrease: %v->%v",rf.commitIndex,idx)
	}
	rf.commitIndex = idx
}

func (rf *Raft) getLastCommitTerm() int {
	//rf.logMu.Lock()
	//defer rf.logMu.Unlock()

	return rf.commitTerm
}

func (rf *Raft) getLastLogEntryTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}

func (rf *Raft) getLastLogEntryIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) setLeader(leaderID int) {
	//rf.leaderMu.Lock()
	//defer rf.leaderMu.Unlock()

	rf.currentLeader = leaderID
}

func (rf *Raft) getLeader() int {
	//rf.leaderMu.Lock()
	//defer rf.leaderMu.Unlock()

	return rf.currentLeader
}

func (rf *Raft) setVotedFor(votedFor int) {
	if rf.votedFor != votedFor{
		rf.votedFor = votedFor
		rf.persist()
	}
}

func (rf *Raft) resetVotedFor() {
	rf.setVotedFor(-1)
}

func (rf *Raft) getVotedFor() int {
	return rf.votedFor
}

func (rf *Raft) noVoted() bool {
	return rf.getVotedFor() == -1
}
