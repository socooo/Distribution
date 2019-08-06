package shardkv

import "fmt"

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	fmt.Printf("in kv RPC, get.\n")
	op := DataOp{RequestType:Get, Key: args.Key, RequestIndex: args.RequestIndex, ClientId: args.ClientId}
	handleRep := kv.handleRequest(op)
	reply.Err = handleRep.err
	reply.Value = handleRep.value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	fmt.Printf("in kv RPC, PutAppend.\n")
	var requestType opType
	if args.Op == "Append"{
		requestType = Append
	} else {
		requestType = Put
	}
	op := DataOp{RequestType:requestType, Key: args.Key, Value: args.Value, RequestIndex: args.RequestIndex, ClientId: args.ClientId}
	handleRep := kv.handleRequest(op)
	reply.Err = handleRep.err
}

func (kv *ShardKV) ShardsMigrating(args *MigratingArgs, reply *MigratingReply){
	kv.mu.Lock()
	if args.Config.Num < kv.lastConfig.Num{
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if args.Config.Num == kv.lastConfig.Num{
		op := ShardOp{RequestType: SendShards, Data: args.Data, Config: args.Config, Shards: args.Shards}
		kv.mu.Unlock()
		handleRep := kv.handleRequest(op)
		reply.Err = handleRep.err
	}
	fmt.Printf("in kv RPC, ShardsMigrating, gid: %v, leader: %v, me: %v, reply: %v.\n", kv.gid, kv.rf.VotedFor, kv.me, reply)
}