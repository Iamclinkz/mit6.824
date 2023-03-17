package raft

import "time"

// 如果当前不是leader，那么sleep+判断当前的心跳有没有超时，如果超时则向信道发送需要选举命令
func (rf *Raft) startFollowerHeartBeatCheckTicker() {
	for rf.killed() == false {
		time.Sleep(getRandFollowerHeartBeatCheckDuration())
		if rf.isFollower() && rf.heartBeatExpire() {
			select {
			case <-rf.closeCh:
				rf.log(dTimer, "FollowerHeartBeatCheckTicker stop working")
				return
			case rf.needElectionCh <- struct{}{}:
			}
		}
	}
}

//如果当前是leader，那么需要定时检查有没有给follower心跳（发送AppendEntries rpc）
func (rf *Raft) startLeaderHeartBeatTicker() {
	for rf.killed() == false {
		time.Sleep(getLeaderHeartBeatDuration())
		if rf.isLeader() {
			select {
			case <-rf.closeCh:
				rf.log(dTimer, "LeaderHeartBeatTicker stop working")
				return
			case rf.needHeartBeat <- struct{}{}:
			}
		}
	}
}
