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

type LogTermList struct {
	head *logTermNode
	tail *logTermNode
	rf *Raft		//为了使用log
}

//newLogTermList 创建一个有头节点的list
func newLogTermList(raft *Raft) *LogTermList {
	return &LogTermList{
		head: &logTermNode{
			term: -1,
			cmds: nil,
			next: nil,
		},
		tail: nil,
		rf: raft,
	}
}

//addTerm 只有leader调用，每次某个term自己上任leader的时候，需要保存一个当前term拥有的log的map
//这样如果有些日志发给了其他peer -> 自己变成follower -> 因为新leader没有这些日志，所以删除掉这些日志 ->
//↑中提到的"其他peer"竞选成功，当上了leader -> 把删除的日志重新发回来 -> commit成功
//后，如果我们没有保存NotifyCh，那么就会返回给Start()一个错误的结果（客户端以为没有应用，但是实际上应用了）
func (list *LogTermList) addTerm(term int)  {
	p := list.head
	for p.next != nil{
		//防止写错，简单搞个check
		if p.term == term{
			list.rf.log(dError,"add term called more than once!")
			panic("")
		}
		p = p.next
	}

	p.next = &logTermNode{
		term: term,
		cmds: make(map[int]*CommandWithNotifyCh),
		next: nil,
	}
	list.rf.log(dLog2,"leader add term:%v to termList",term)
	list.tail = p.next
}

func (list *LogTermList) leaderAddLogEntry(term,idx int,withNotify *CommandWithNotifyCh)  {
	//简单做个check
	if list.tail.term != term{
		list.rf.log(dError,"tail.term:%v, requestTerm:%v", list.tail.term, term)
		panic("")
	}

	list.rf.log(dLog2,"leader add log to termList, index:%v,term:%v",idx,term)
	list.tail.cmds[idx] = withNotify
}

func (list *LogTermList)commitLogToClient(term int,idx int){
	p := list.head.next
	for p != nil && p.term < term{
		p = p.next
	}

	if p == nil || p.term != term{
		return
	}

	if cmd,ok := p.cmds[idx];ok{
		cmd.finishWithOK(term,idx)
		list.rf.log(dClient,"finishWithOK: term:%v, log%v", term,idx)
	}

	//当前已经到了p.term，需要删除掉head -> p之间的所有节点（即删除掉old term的内容）
	p2 := list.head.next
	for p2 != p{
		for errorLogIdx, withNotifyCh := range p2.cmds {
			list.rf.log(dClient,"finishWithError: term:%v, log%v", p2.term,errorLogIdx)
			withNotifyCh.finishWithError()
		}
		list.head.next = p2.next
		p2 = p2.next
	}
}

