package raft

import (
	"math/rand"
	"time"
)

//getRandFollowerHeartBeatCheckDuration 作为follower，多长时间检查一次心跳有没有过期
func getRandFollowerHeartBeatCheckDuration() time.Duration {
	return time.Duration(200+rand.Int()%100) * time.Millisecond
}

//getFollowerHeartBeatExpireTime 如果多长时间没有收到来自leader的心跳，则判断失败
func getFollowerHeartBeatExpireTime() time.Duration {
	return time.Duration(300) * time.Millisecond
}

//getLeaderHeartBeatDuration 作为follower，多长时间检查一次心跳有没有过期
func getLeaderHeartBeatDuration() time.Duration {
	return time.Duration(50) * time.Millisecond
}

//getRandCandidateOverTimeTickDuration 作为candidate，多长时间选举过期，进行下一轮选举
func getRandCandidateOverTimeTickDuration() time.Duration {
	return time.Duration(200+rand.Int()%100) * time.Millisecond
}
