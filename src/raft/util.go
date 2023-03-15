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

//termHashMap 用于快速索引某一个term对应到log的下标
type termHashMap struct {
	maxTerm int
	nodes map[int]*termHashMapNode
}

type termHashMapNode struct {
	term int

	//注意，[from,to]中的内容都是第term的
	from int
	to	int
}

//rebuild 重新构建termHashMap，entries[0,commitIndex]的内容一定和之前相同，所以不需要重新刷新
func (m *termHashMap) rebuild(entries []*LogEntry,commitIndex int){
	if entries == nil{
		panic("entries should not be null")
	}

	if len(entries) == 0{
		return
	}

	for m.maxTerm == entries[len(entries) - 1].Term{
		return
	}


}

//getTermBegin 辅助函数，获取 entries[len(entries) - 1].Term 是从哪里开始的
func getTermBegin(entries []*LogEntry)int{
	if entries == nil || len(entries) == 0{
		panic("program fault!")
	}

	term := entries[len(entries) - 1].Term
	i := len(entries) - 1
	for ;i >= 0; i-- {
		if entries[i].Term != term{
			break
		}
	}

	return i + 1
}

//getTermEnd 辅助函数，获取 entries[0].Term 是从哪里结束的
func getTermEnd(entries []*LogEntry)int{
	if entries == nil || len(entries) == 0{
		panic("program fault!")
	}

	term := entries[0].Term
	i := 0
	for ;i < len(entries); i++ {
		if entries[i].Term != term{
			break
		}
	}

	return i - 1
}
