package raft

//投送到任意一个rpc的chan的msg
type rpcChMsg struct {
	//rpc的请求，为了避免拷贝，这里直接传Rpc请求的指针了
	RpcReq any
	//rpc的回复，为了避免拷贝，这里直接传Rpc回复的指针了
	RpcResp any
	//处理结束，用于通知等待结果的go程的chan，如果有错误，通过chan返回
	RespCh chan error
}

func newRpcChMsg(rpcReq any, rpcResp any) *rpcChMsg {
	return &rpcChMsg{
		RpcReq:  rpcReq,
		RpcResp: rpcResp,
		RespCh:  make(chan error, 1),
	}
}

func pushRpcChan(rpcReq any, rpcResp any, targetCh chan *rpcChMsg) chan error {
	msg := newRpcChMsg(rpcReq, rpcResp)
	targetCh <- msg
	return msg.RespCh
}

func (msg rpcChMsg) finish(err error) {
	msg.RespCh <- err
}
