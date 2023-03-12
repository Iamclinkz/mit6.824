package raft

func (rf *Raft) goF(f func(), name string) {
	// Your code here (2A, 2B).
	rf.wg.Add(1)
	go func() {
		//rf.log(dInfo,"%v started",name)
		f()
		rf.wg.Done()
		//rf.log(dInfo,"%v finished",name)
	}()
}

//var leaderHeartBeatStartTicker *time.Ticker = time.NewTicker(LeaderHeartBeatDuration)
//var followerHeartBeatCheckTicker *time.Ticker = time.NewTicker(LeaderHeartBeatDuration)
