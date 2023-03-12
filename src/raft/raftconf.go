package raft

import (
	"math/rand"
	"time"
)

//getRandFollowerHeartBeatCheckDuration 作为follower，多长时间检查一次心跳有没有过期
func getRandFollowerHeartBeatCheckDuration() time.Duration {
	return time.Duration(200+rand.Int()%200) * time.Millisecond
}

//getFollowerHeartBeatExpireTime 如果多长时间没有收到来自leader的心跳，则判断失败
func getFollowerHeartBeatExpireTime() time.Duration {
	return time.Duration(400) * time.Millisecond
}

//getLeaderHeartBeatDuration 作为follower，多长时间检查一次心跳有没有过期
func getLeaderHeartBeatDuration() time.Duration {
	return time.Duration(100) * time.Millisecond
}

//getRandCandidateOverTimeTickDuration 作为follower，多长时间检查一次心跳有没有过期
func getRandCandidateOverTimeTickDuration() time.Duration {
	return time.Duration(200+rand.Int()%200) * time.Millisecond
}
