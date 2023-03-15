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

type logTermNode struct {
	term int
	cmds map[int]*CommandWithNotifyCh
	next *logTermNode
}

type logTermList struct {
	head *logTermNode
}

//newLogTermList 创建一个有头节点的list
func newLogTermList() *logTermList {
	return &logTermList{
		head: &logTermNode{
			term: -1,
			cmds: nil,
			next: nil,
		},
	}
}

//removeTermTo 把term == to之前的全部cmds删除，并且返回error
func (list *logTermList) removeTermTo(to int)  {
	for list.head.next != nil && list.head.next.term != to{
		for _, withNotifyCh := range list.head.next.cmds {
			withNotifyCh.finishWithError()
		}
		list.head.next = list.head.next.next
	}
}

//addTerm 只有leader调用，每次某个term自己上任leader的时候，需要保存一个当前term的map，
func (list *logTermList) addTerm(term int)  {
	p := list.head
	for p.next != nil{
		//防止写错，简单搞个check
		if p.term == term{
			panic("add term called more than once!")
		}
		p = p.next
	}

	p.next = &logTermNode{
		term: term,
		cmds: make(map[int]*CommandWithNotifyCh),
		next: nil,
	}
}

func (list *logTermList)commitLogToClient(term int,idx int){
	p := list.head.next
	for p != nil{
		if p.term == term{
			if cmd,ok := p.cmds[idx];ok{
				cmd.finishWithOK(term,idx)
			}
		}
	}
}

