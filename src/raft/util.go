package raft

import "errors"

func (rf *Raft) goF(f func(), name string) {
	// Your code here (2A, 2B).
	rf.wg.Add(1)
	go func() {
		//rf.log(dInfo,"%v started",name)
		f()
		rf.wg.Done()
		rf.log(dInfo, "%v finished", name)
	}()
}

//var leaderHeartBeatStartTicker *time.Ticker = time.NewTicker(LeaderHeartBeatDuration)
//var followerHeartBeatCheckTicker *time.Ticker = time.NewTicker(LeaderHeartBeatDuration)

//QuickSort from chatGPT的快排实现
func QuickSort(arr []int, left, right int) {
	if left >= right {
		return
	}
	i, j := left, right
	pivot := arr[left]
	for i < j {
		for i < j && arr[j] >= pivot {
			j--
		}
		arr[i] = arr[j]
		for i < j && arr[i] <= pivot {
			i++
		}
		arr[j] = arr[i]
	}
	arr[i] = pivot
	QuickSort(arr, left, i-1)
	QuickSort(arr, i+1, right)
}

func readChAndThrow(ch chan struct{}) {
	select {
	case <-ch:
	default:
		return
	}
}

//readChAndThrowUntilEmpty 读空所有的commandCh的内容，并且向上层返回error
func (rf *Raft) readChAndThrowUntilEmpty() {
	for {
		select {
		case cmd := <-rf.commandCh:
			cmd.finishWithError()
		case cmd := <-rf.appendEntriesReqCh:
			cmd.finish(killedError)
		case cmd := <-rf.requestVoteReqCh:
			cmd.finish(killedError)
		case <-rf.appendEntriesReplyCh:
		case <-rf.requestVoteReplyCh:

		default:
			return
		}
	}
}

var killedError error = errors.New("killed error")
