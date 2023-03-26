package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

//OpID 用于全局唯一的标识一个Op的结构
type OpID struct {
	ClientSequenceNum int64 //ClientID表示的客户端的对指令的编号
	ClientID          int64 //唯一的表示一个客户端
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Type  string // "Put" or "Append"
	OpID  OpID   //来自客户端的，唯一的表示Op的ID
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   string
	Value string
}
