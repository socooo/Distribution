package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import (
	"encoding/gob"
	"time"
	"log"
	"fmt"
	"bytes"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.forKill)
}

func (kv *ShardKV) handleRequest(op interface{}) handleReply{
	reply := handleReply{}
	if dataOp, ok := op.(DataOp); ok{
		kv.mu.Lock()
		shardIndex := key2shard(dataOp.Key)
		fmt.Printf("in kv handleRequest, gid:%v, configNum: %v, shard index: %v, shards state: %v.\n", kv.gid, kv.lastConfig.Num, shardIndex, kv.shardsState)
		switch kv.shardsState[shardIndex]{
		case NotAvailable:
			reply.err = ErrWrongGroup
			return reply
		case DataExporting:
			reply.err = MigratingData
			return reply
		case DataImporting:
			reply.err = MigratingData
			return reply
		}
		kv.mu.Unlock()
	}
	index, term, isLeader := kv.rf.Start(op)
	if isLeader{
		waitApplyCh := make(chan shardApplyMsg)
		kv.mu.Lock()
		kv.responseHandler[index] = waitApplyCh
		kv.mu.Unlock()
		select {
		case msg := <- waitApplyCh:
			kv.mu.Lock()
			delete(kv.responseHandler, index)
			kv.mu.Unlock()
			if kv.rf.CurrentTerm == term{
				reply.err = msg.err
				reply.value = msg.value
				kv.lastRequest[msg.clientId] = msg.requestIndex
			} else {
				reply.err = WrongLeader
			}
			return reply
		case <- time.After(ShardApplyTimeOut):
			reply.err = TimeOut
			kv.mu.Lock()
			delete(kv.responseHandler, index)
			kv.mu.Unlock()
			return reply
		}
	} else {
		reply.err = WrongLeader
		return reply
	}
}

func (kv *ShardKV) commandApply(raftApplyMsg raft.ApplyMsg){
	opInMsg := raftApplyMsg.Command
	var msg shardApplyMsg
	switch opInMsg.(type){
	case DataOp:
		msg = kv.applyDataOp(opInMsg.(DataOp))
	case ConfigOp:
		msg = kv.applyConfigOp(opInMsg.(ConfigOp))
	case ShardOp:
		msg = kv.applyShardOp(opInMsg.(ShardOp))
	default:
		log.Fatalf("Error commad type.\n")
	}
	if handler, isExist := kv.responseHandler[raftApplyMsg.Index]; isExist{
		//fmt.Printf("in commandApply, send handler index: %v, handler content: %v.\n", raftApplyMsg.Index, kv.responseHandler[raftApplyMsg.Index])
		handler <- msg
	}
}

func (kv *ShardKV) applyDataOp(op DataOp) shardApplyMsg{
	msg := shardApplyMsg{}
	//fmt.Printf("in commandApply, me: %v, handler index: %v.\n", kv.me, raftApplyMsg.Index)
	shardIndex := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.RequestType == Get{
		result, ok := kv.data[shardIndex][op.Key];
		if ok{
			msg = shardApplyMsg{value: result, err:OK}
			fmt.Printf("in kv server commandApply, gid: %v, me: %v,get data: %v.\n", kv.gid, kv.me, result)
		} else {
			msg = shardApplyMsg{err: ErrNoKey}
		}
	} else {
		lastRequest, ok := kv.lastRequest[op.ClientId]
		if !ok || lastRequest < op.RequestIndex{
			if op.RequestType == Put{
				fmt.Printf("in kv apply put,gid: %v, data: %v.\n",kv.gid, kv.data)
				kv.data[shardIndex][op.Key] = op.Value
				msg = shardApplyMsg{err: OK}
			} else if op.RequestType == Append{
				kv.data[shardIndex][op.Key] += op.Value
				//fmt.Printf("in server commandApply, me: %v, append data: %v.\n", kv.me, op.Value)
				msg = shardApplyMsg{err: OK}
			} else {
				log.Fatal("Error type.")
			}
		} else {
			msg = shardApplyMsg{err: OK}
		}
	}
	return msg
}

func (kv *ShardKV) applyConfigOp(op ConfigOp) shardApplyMsg{
	msg := shardApplyMsg{}
	term, isLeader := kv.rf.GetState()

	dataMigrating := func() bool{
		for _, state := range kv.shardsState{
			if state == DataExporting || state == DataImporting{
				return true
			}
		}
		return false
	}()

	if !isLeader || term != kv.rf.CurrentTerm{
		msg.err = WrongLeader
		return msg
	}

	if op.ShardsConfig.Num != kv.lastConfig.Num + 1 || dataMigrating{
		msg.err = MigratingData
		return msg
	}

	newConfig := op.ShardsConfig
	kv.lastConfig = newConfig
	newShardsInfo := newConfig.Shards	// shardIndex -> gid
	migratingData := make(map[int]map[int]map[string]string, 0)	// gid -> kv.data

	if newConfig.Num == 1{
		kv.mu.Lock()
		for shardIndex, gidInNewConfig := range newShardsInfo{
			if gidInNewConfig == kv.gid{
				kv.shardsState[shardIndex] = Available
			}
		}
		kv.mu.Unlock()
	} else {
		kv.mu.Lock()
		for shardIndex, gidInNewConfig := range newShardsInfo{
			if gidInNewConfig == kv.gid && kv.shardsState[shardIndex] == NotAvailable{
				kv.shardsState[shardIndex] = DataImporting
			} else if gidInNewConfig != kv.gid && kv.shardsState[shardIndex] == Available{
				kv.shardsState[shardIndex] = DataExporting
				if migratingData[gidInNewConfig] == nil {
					migratingData[gidInNewConfig] = make(map[int]map[string]string)
				}
				if migratingData[gidInNewConfig][shardIndex] == nil {
					migratingData[gidInNewConfig][shardIndex] = make(map[string]string)
				}
				// 防止并发冲突，拷贝值而不是拷贝map引用。
				for key, value := range kv.data[shardIndex]{
					migratingData[gidInNewConfig][shardIndex][key] = value
				}
			}
		}
		kv.mu.Unlock()
		fmt.Printf("in kv applyConfigOp, gid: %v, config num: %v, migratingData: %v.\n", kv.gid, newConfig.Num, migratingData)
		if len(migratingData) > 0{
			go kv.handleDataMigrate(migratingData, newConfig)
		}
	}
	msg.err = OK
	return msg
}

func (kv *ShardKV) handleDataMigrate(migratingData map[int]map[int]map[string]string, newConfig shardmaster.Config){
	term, isLeader := kv.rf.GetState()
	if term != kv.rf.CurrentTerm || !isLeader{
		return
	}
	groupInfo := newConfig.Groups
	newShardsInfo := newConfig.Shards
	fmt.Printf("in kv handleDataMigrate, gid: %v, migratingData: %v.\nconfig: %v.\n", kv.gid, migratingData, newConfig)
	for gid, data := range migratingData{
		servers := groupInfo[gid]
		reply := MigratingReply{}
		shards := make([]int, 0)	// 当前操作影响到的 shards
		for shardIndex, gidInShardsInfo := range newShardsInfo{
			if gid == gidInShardsInfo{
				shards = append(shards, shardIndex)
			}
		}
		args := MigratingArgs{Data: data, Config: newConfig, Shards: shards}
		pollNum := 0
		for reply.Err != OK{
			srv := kv.make_end(servers[pollNum%len(servers)])
			srv.Call("ShardKV.ShardsMigrating", &args, &reply)
			fmt.Printf("in kv handleDataMigrate, after rpc call, gid: %v, reply: %v, args; %v.\n", kv.gid, reply, args)
			if reply.Err == WrongLeader{
				pollNum += 1
			}
		}

		if reply.Err == OK{
			migratingDoneOp := ShardOp{RequestType: CleanShards, Config: newConfig, Shards: shards}
			kv.handleRequest(migratingDoneOp)
		}
	}
}

func (kv *ShardKV) applyShardOp(op ShardOp) shardApplyMsg{
	msg := shardApplyMsg{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("in kv applyShardOp, gid: %v, op: %v.\n", kv.gid, op)

	switch op.RequestType {
	case SendShards:
		for _, shardIndex := range op.Shards{
			if op.Data[shardIndex] == nil{
				kv.shardsState[shardIndex] = Available
			} else {
				kv.data[shardIndex] = op.Data[shardIndex]
				kv.shardsState[shardIndex] = Available
			}
		}
		msg.err = OK
	case CleanShards:
		for _, shardIndex := range op.Shards{
			kv.shardsState[shardIndex] = NotAvailable
		}
		msg.err = OK
	default:
		log.Fatalf("in kv applyShardOp, gid: %v, no such request type: %v.\n", kv.gid, op.RequestType)
	}
	fmt.Printf("in kv applyShardOp, finish, gid: %v, msg: %v, op: %v, shards state: %v.\n\tdata: %v.\n", kv.gid, msg, op, kv.shardsState,kv.data)

	return msg
}

func (kv *ShardKV) run(){
	for{
		select {
		case raftApplyMsg := <- kv.raftApplyCh:
			//fmt.Printf("in kv, receive, gid: %v, me: %v, raft response: %v， len of chan:%v.\n", kv.gid,
			//	kv.me,
			//	raftApplyMsg,
			//	len(kv.raftApplyCh))

			//fmt.Printf("handler: %v.\n", kv.responseHandler)
			if raftApplyMsg.UseSnapshot{
				//fmt.Printf("receive use snapshot, start load snapshot, gid:%v, me: %v, kvdata: %v.\n", kv.gid, kv.me, kv.data)
				kv.loadSnapshot(raftApplyMsg.Snapshot)
			} else {
				kv.commandApply(raftApplyMsg)
				if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate{
					//fmt.Printf("in kv, start snapshot, gid:%v, me: %v, index: %v.\n", kv.gid, kv.me, raftApplyMsg.Index)
					kv.createSnapshot(raftApplyMsg.Index)
				}
			}
		case <- kv.forKill:
			return
		}
	}
}

func (kv *ShardKV) loadSnapshot(data []byte){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	b := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(b)
	snapData := RaftKvData{}
	decoder.Decode(&snapData)
	kv.data = snapData.Data
	kv.lastRequest = snapData.LastRequest
	kv.lastConfig = snapData.LastConfig
	kv.shardsState = snapData.ShardState
	//fmt.Printf("In kv, load snapshot, me: %v, kv data: %v.\n", kv.me, kv.data)
}

func (kv *ShardKV) createSnapshot(index int){
	kv.rf.Mu.Lock()
	defer kv.rf.Mu.Unlock()
	b := new(bytes.Buffer)
	encoder := gob.NewEncoder(b)

	encoder.Encode(RaftKvData{Data: kv.data, LastRequest: kv.lastRequest, LastConfig: kv.lastConfig, ShardState: kv.shardsState})
	saveData := b.Bytes()
	kv.rf.Persister.SaveSnapshot(saveData)
	kv.rf.LogCompact(index)
}

func (kv *ShardKV) masterPolling(){
	for{
		select {
		case <- time.After(PollingInterval):
			if _, isLeader := kv.rf.GetState(); !isLeader{
				continue
			}
			newConfig := kv.mck.Query(-1)
			lastConfigNum := kv.lastConfig.Num

			if newConfig.Num == lastConfigNum + 1{
				fmt.Printf("in kv master polling, get config, gid: %v, reply: %v, configNum: %v.\n", kv.gid, newConfig, newConfig.Num)
				op := ConfigOp{ShardsConfig: newConfig}
				reply := handleReply{}
				for reply.err != OK{
					reply = kv.handleRequest(op)
					if newConfig.Num <= kv.lastConfig.Num{
						reply.err = OK
					}
				}
			} else if newConfig.Num > lastConfigNum + 1{
				for i := lastConfigNum + 1; i <= newConfig.Num; i++{
					currentNewConfig := kv.mck.Query(i)
					op := ConfigOp{ShardsConfig:currentNewConfig}
					reply := handleReply{}
					fmt.Printf("in kv master polling, get config, gid: %v, config num: %v, current config num: %v, shardState: %v.\n", kv.gid, newConfig.Num, currentNewConfig.Num, kv.shardsState)
					for reply.err != OK{
						reply = kv.handleRequest(op)
						if currentNewConfig.Num <= kv.lastConfig.Num{
							reply.err = OK
						}
						fmt.Printf("in kv master polling, not OK. gid: %v, me: %v, handle request reply: %v, get configNum: %v, current configNum: %v, shardState: %v.\n", kv.gid, kv.me, reply, currentNewConfig.Num, kv.lastConfig.Num, kv.shardsState)
					}
				}
			}else {
				//fmt.Printf("in kv master polling, get config, gid: %v,configNum: %v, kv.configNum == newConfigNum .\n", kv.gid, newConfig.Num)
			}
		case <- kv.forKill:
			return
		}
	}
}
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {

	gob.Register(DataOp{})
	gob.Register(ConfigOp{})
	gob.Register(ShardOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[int]map[string]string)
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.raftApplyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.raftApplyCh)
	kv.responseHandler = make(map[int] chan shardApplyMsg)
	kv.lastRequest = make(map[int64]int)
	kv.forKill = make(chan struct{})
	kv.mck = shardmaster.MakeClerk(kv.masters)

	for i:=0; i < shardmaster.NShards;i++{
		kv.data[i] = make(map[string]string)
		kv.shardsState[i] = NotAvailable
	}
	snapData := kv.rf.Persister.ReadSnapshot()
	if kv.maxraftstate != -1 && snapData != nil && len(snapData) > 0{
		kv.loadSnapshot(snapData)
	}

	go kv.masterPolling()
	go kv.run()
	return kv
}
