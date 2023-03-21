package raft

import (
	"log"
	"sync/atomic"
)

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

// 只有主线程可以set
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

// setTerm 设置当前的term，必须保证前后不同
func (rf *Raft) setTerm(term int) {
	oldTerm := rf.getTerm()

	if oldTerm != term {
		if oldTerm > term {
			rf.log(dError, "old term should not greater than new term!")
			panic("")
		}
		rf.log(dTerm, "term: %v -> %v", oldTerm, term)
		atomic.StoreInt32(&rf.currentTerm, int32(term))
		//2C：这里改变VotedFor的时候，同时也持久化了，所以在外面不再进行持久化
		rf.resetVotedFor()
		rf.thisTermMatchedLeader = false
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
	return rf.commitIndex
}

func (rf *Raft) setLastCommitIdx(idx int) {
	//简单做个check
	if idx < rf.commitIndex {
		rf.log(dError, "last commit index should not decrease: %v->%v", rf.commitIndex, idx)
	}
	rf.commitIndex = idx
}

func (rf *Raft) getLastLogEntryTerm() int {
	return rf.logEntries.GetLastLogEntryTerm()
}

func (rf *Raft) getLastLogEntryIndex() int {
	return rf.logEntries.GetLastLogEntryIndex()
}

func (rf *Raft) setVotedFor(votedFor int) {
	if rf.votedFor != votedFor {
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

// LogEntries 2D中对LogEntry做的封装
type LogEntries struct {
	//被快照压缩的最后一条日志的index，初始值为0
	LastIncludeIndex int
	//Logs 存放实际的log的切片。注意，使用logs[0]作为哨兵！Logs[0].Term = lastIncludeTerm
	//如果当前没有快照，那么logs[0].Term = -1
	Logs []*LogEntry
}

// Reinit 使用两个参数，重建LogEntries
func (es *LogEntries) Reinit(lastIncludeTerm, lastIncludeIndex int) {
	es.LastIncludeIndex = lastIncludeIndex
	es.Logs = make([]*LogEntry, 1)
	es.Logs[0] = &LogEntry{
		Command: nil,
		Term:    lastIncludeTerm,
	}
}

func (es *LogEntries) snapshotLogEntry(lastIncludeIndex int) {

}

func (es *LogEntries) GetLastLogEntryTerm() int {
	return es.Logs[len(es.Logs)-1].Term
}

func (es *LogEntries) GetLastIncludeTerm() int {
	return es.Logs[0].Term
}

func (es *LogEntries) GetLastLogEntryIndex() int {
	return len(es.Logs) + es.LastIncludeIndex - 1
}

func (es *LogEntries) GetFirstLogEntryIndex() int {
	return es.LastIncludeIndex + 1
}

func (es *LogEntries) Len() int {
	return len(es.Logs) + es.LastIncludeIndex
}

func (es *LogEntries) Get(idx int) *LogEntry {
	if idx <= es.LastIncludeIndex || idx > es.GetLastLogEntryIndex() {
		return nil
	}

	return es.Logs[idx-es.LastIncludeIndex]
}

// GetCopy 获取 [from,es.Len()-1] 的LogEntry的切片的copy
func (es *LogEntries) GetCopy(from int) []*LogEntry {
	if from > es.Len() {
		log.Panicf("from(%v) should not bigger than es.Len(%v)", from, es.Len())
	}

	if from <= es.LastIncludeIndex {
		log.Panicf("from(%v) should not less than or equals to LastIncludeIdx(%v)", from, es.LastIncludeIndex)
	}

	ret := make([]*LogEntry, es.Len()-from)
	copy(ret, es.Logs[from-es.LastIncludeIndex:])
	return ret
}

// AppendCommand 新加一条日志，返回添加的位置
func (es *LogEntries) AppendCommand(entry *LogEntry) int {
	es.Logs = append(es.Logs, entry)
	return es.GetLastLogEntryIndex()
}

// RemoveCommandUntil 删除从最后一条，一直到idx（不包括idx）的日志，返回最后一条日志的位置
func (es *LogEntries) RemoveCommandUntil(idx int) int {
	if idx > es.GetLastLogEntryIndex() || idx <= es.LastIncludeIndex {
		log.Panicf("should not remove command until:%v, because LastIncludeIndex:%v, lastLogEntryIndex:%v",
			idx, es.LastIncludeIndex, es.GetLastLogEntryIndex())
	}

	es.Logs = es.Logs[:idx-es.LastIncludeIndex+1]
	return es.GetLastLogEntryIndex()
}

// AppendCommands 将日志切片加入到logs的后面，返回最后一条日志的位置
func (es *LogEntries) AppendCommands(entries []*LogEntry) int {
	es.Logs = append(es.Logs, entries...)
	return es.GetLastLogEntryIndex()
}

func NewLogEntries() *LogEntries {
	return &LogEntries{
		LastIncludeIndex: 0,
		Logs:             make([]*LogEntry, 0),
	}
}
