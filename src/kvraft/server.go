package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0
const KVApplyTimeOut = 1 * time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	RequestType opType
	Key string
	Value string
	RequestIndex int
	ClientId int64
}

type RaftKV struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	raftApplyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	data map[string]string
	responseHandler map[int] chan kvApplyMsg
	lastPutAppendRequest map[int64] int

	forKill chan struct{}
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{RequestType:Get, Key: args.Key, RequestIndex: args.RequestIndex, ClientId: args.ClientId}
	index, term, isLeader := kv.rf.Start(op);
	//fmt.Printf("in kv get, isleader: %v, args: %v, me:%v.\n", isLeader, args,kv.me)
	if isLeader{
		waitApplyCh := make(chan kvApplyMsg)
		kv.mu.Lock()
		kv.responseHandler[index] = waitApplyCh
		kv.mu.Unlock()
		//fmt.Printf("in kv get, index: %v, isleader: %v, args: %v, me:%v, handler: %v.\n", index, isLeader, args,kv.me, kv.responseHandler)
		select {
		case kvApplyMsg := <- waitApplyCh:
			kv.mu.Lock()
			//fmt.Printf("kv get received. deleting response handler: %v.\n", index)
			delete(kv.responseHandler, index)
			kv.mu.Unlock()
			if kv.rf.CurrentTerm == term{
				reply.WrongLeader = false
				reply.Value = kvApplyMsg.value
				reply.Err = OK
			} else {
				reply.WrongLeader = true
				reply.Err = WrongLeader
			}
		case <- time.After(KVApplyTimeOut):
			reply.WrongLeader = false
			reply.Err = TimeOut
			//newTerm, stillLeader := kv.rf.GetState()
			//fmt.Printf("in Get, time out, me: %v, term: %v, stillLeader: %v.\n", kv.me, newTerm, stillLeader)
			kv.mu.Lock()
			//fmt.Printf("kv get time out, deleting response handler: %v.\n", index)
			delete(kv.responseHandler, index)
			kv.mu.Unlock()
			return
		}
	} else {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var requestType opType
	if args.Op == "Append"{
		requestType = Append
	} else {
		requestType = Put
	}
	op := Op{RequestType:requestType, Key: args.Key, Value: args.Value, RequestIndex: args.RequestIndex, ClientId: args.ClientId}
	index, term, isLeader := kv.rf.Start(op)
	//fmt.Printf("in kv putappend, isleader: %v, args: %v, me: %v.\n", isLeader, args, kv.me)
	if isLeader{
		waitApplyCh := make(chan kvApplyMsg)
		kv.mu.Lock()
		kv.responseHandler[index] = waitApplyCh
		kv.mu.Unlock()
		//fmt.Printf("in kv putappend, index: %v, args: %v, me: %v, handler: %v.\n", index, args, kv.me, kv.responseHandler)

		select {
		case <- waitApplyCh:
			kv.mu.Lock()
			//fmt.Printf("kv putAppend received, deleting response handler: %v.\n", index)
			delete(kv.responseHandler, index)
			kv.mu.Unlock()
			if kv.rf.CurrentTerm == term{
				reply.Err = OK
				reply.WrongLeader = false
				//fmt.Printf("in kv putappend, data[%v]'s content: %v.\n", args.Key, kv.data[args.Key])
				return
			} else {
				reply.WrongLeader = true
				reply.Err = WrongLeader
				return
			}
		case <- time.After(KVApplyTimeOut):
			kv.mu.Lock()
			//fmt.Printf("kv putappend time out, deleting response handler: %v.\n", index)
			delete(kv.responseHandler, index)
			kv.mu.Unlock()
			//newTerm, stillLeader := kv.rf.GetState()
			//fmt.Printf("in PutAppend, time out, me: %v, term: %v, stillLeader: %v.\n", kv.me, newTerm, stillLeader)
			reply.WrongLeader = false
			reply.Err = TimeOut
			return
		}
	} else {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	close(kv.forKill)
}

func (kv *RaftKV) commandApply(raftApplyMsg raft.ApplyMsg){
	msg := kvApplyMsg{}
	//fmt.Printf("in commandApply, me: %v, handler index: %v.\n", kv.me, raftApplyMsg.Index)
	if args, ok := raftApplyMsg.Command.(Op); ok{
		if args.RequestType == Get{
			kv.mu.RLock()
			result, ok := kv.data[args.Key];
			kv.mu.RUnlock()
			if ok{
				msg = kvApplyMsg{value: result, err:OK}
			} else {
				msg = kvApplyMsg{err: ErrNoKey}
			}
		} else {
			kv.mu.RLock()
			lastRequest, ok := kv.lastPutAppendRequest[args.ClientId]
			kv.mu.RUnlock()
			if !ok || lastRequest < args.RequestIndex{
				if args.RequestType == Put{
					kv.mu.Lock()
					kv.data[args.Key] = args.Value
					kv.mu.Unlock()
					msg = kvApplyMsg{err: OK}
				} else if args.RequestType == Append{
					kv.mu.Lock()
					kv.data[args.Key] += args.Value
					kv.mu.Unlock()
					msg = kvApplyMsg{err: OK}
				} else {
					log.Fatal("Error type.")
				}
			} else {
				msg = kvApplyMsg{err: OK}
			}
		}
		if handler, isExist := kv.responseHandler[raftApplyMsg.Index]; isExist{
			//fmt.Printf("in commandApply, send handler index: %v, handler content: %v.\n", raftApplyMsg.Index, kv.responseHandler[raftApplyMsg.Index])
			handler <- msg
		}
		kv.mu.Lock()
		kv.lastPutAppendRequest[args.ClientId] = args.RequestIndex
		kv.mu.Unlock()
	} else {
		log.Fatal("Error type.")
	}
}

func (kv *RaftKV) run(){
	for{
		select {
		case raftApplyMsg := <- kv.raftApplyCh:
			//fmt.Printf("receive, me: %v, raft response: %v， len of chan:%v, me: %v.\n",
			//	kv.me,
			//	raftApplyMsg,
			//	len(kv.raftApplyCh), kv.me)

			//fmt.Printf("handler: %v.\n", kv.responseHandler)
			kv.commandApply(raftApplyMsg)
		case <- kv.forKill:
			return
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[string]string)

	kv.raftApplyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.raftApplyCh)
	kv.responseHandler = make(map[int] chan kvApplyMsg)
	kv.lastPutAppendRequest = make(map[int64]int)
	kv.forKill = make(chan struct{})
	go kv.run()
	return kv
}
