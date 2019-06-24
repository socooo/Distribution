package shardmaster


import "raft"
import "labrpc"
import (
	"encoding/gob"
	"log"
	"time"
	"fmt"
)

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	fmt.Printf("in shard master, start join, args: %v.\n", args)
	if sm.isRequestDuplicate(args.ClientId, args.RequestIndex){
		reply.Err = DuplicateRequest
		return
	}
	handlerResult := sm.handleRequest(sm.rf.Start(*args))
	reply.Err = handlerResult.err
	fmt.Printf("join reply: %v.\n", reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	fmt.Printf("in shard master, start leave, args: %v.\n", args)
	if sm.isRequestDuplicate(args.ClientId, args.RequestIndex){
		reply.Err = DuplicateRequest
		return
	}
	handlerResult := sm.handleRequest(sm.rf.Start(*args))
	reply.Err = handlerResult.err
	fmt.Printf("leave reply: %v.\n", reply)

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	fmt.Printf("in shard master, start move, args: %v.\n", args)
	if sm.isRequestDuplicate(args.ClientId, args.RequestIndex){
		reply.Err = DuplicateRequest
		return
	}
	handlerResult := sm.handleRequest(sm.rf.Start(*args))
	reply.Err = handlerResult.err
	fmt.Printf("move reply: %v.\n", reply)

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	fmt.Printf("in shard master, start query, args: %v.\n", *args)
	if sm.isRequestDuplicate(args.ClientId, args.RequestIndex){
		reply.Err = DuplicateRequest
		return
	}
	index, term, isLeader := sm.rf.Start(*args)
	handlerResult := sm.handleRequest(index, term, isLeader)
	reply.Err = handlerResult.err

	if handlerResult.err == OK{
		sm.mu.Lock()
		if args.Num < 0 || args.Num > len(sm.configs){
			reply.Config = sm.configs[len(sm.configs) - 1]
		} else {
			fmt.Printf("in query, args.num: %v, config: %v, me: %v.\n", args.Num, sm.configs, sm.me)
			reply.Config = sm.configs[args.Num]
		}
		sm.mu.Unlock()
	}
	fmt.Printf("query reply: %v.\n", reply)
}

func (sm *ShardMaster) isRequestDuplicate(clientId int64, requestIndex int) bool{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	lastIndex, ok := sm.lastRequest[clientId]
	return ok && lastIndex == requestIndex
}

func (sm *ShardMaster) handleRequest(index int, term int, isLeader bool) handleReply{
	reply := handleReply{}
	if isLeader{
		waitApplyCh := make(chan shardMasterApplyMsg)
		sm.mu.Lock()
		sm.responseHandler[index] = waitApplyCh
		sm.mu.Unlock()
		fmt.Printf("in handleRequest, create handle:%v.\n", sm.responseHandler)
		select {
		case msg := <- waitApplyCh:
			sm.mu.Lock()
			delete(sm.responseHandler, index)
			sm.mu.Unlock()
			if sm.rf.CurrentTerm == term{
				reply.err = OK
				sm.lastRequest[msg.clientId] = msg.requestIndex
			} else {
				reply.err = WrongLeader
			}
			return reply
		case <- time.After(smApplyTimeout):
			reply.err = TimeOut
			sm.mu.Lock()
			delete(sm.responseHandler, index)
			sm.mu.Unlock()
			return reply
		}
	} else {
		reply.err = WrongLeader
		return reply
	}
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.forKill)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) run(){
	for{
		select {
		case raftApplyMsg := <- sm.raftApplyCh:
			sm.commandApply(raftApplyMsg)
		case <- sm.forKill:
			return
		}
	}
}

func (sm *ShardMaster) commandApply(raftApplyMsg raft.ApplyMsg){
	sm.mu.Lock()
	defer sm.mu.Unlock()

	msg := shardMasterApplyMsg{}
	fmt.Printf("in commandApply, msg: %v, me: %v.\n", raftApplyMsg, sm.me)
	smLastConfig := sm.configs[len(sm.configs) - 1]
	// map 是引用拷贝，需要重新创建 map 对象并赋值。
	var groupCopy = make(map[int] []string)
	for index, group := range smLastConfig.Groups{
		groupCopy[index] = group
	}
	smConfigCopy := Config{
		smLastConfig.Num + 1,
		smLastConfig.Shards,
		groupCopy}

	switch raftApplyMsg.Command.(type) {
	case JoinArgs:
		fmt.Printf("start join apply, me: %v.\n",sm.me)
		joinArgs := raftApplyMsg.Command.(JoinArgs)
		for gid, servers := range joinArgs.Servers{
			smConfigCopy.Groups[gid] = servers
		}
		sm.balanceShardInGroup(&smConfigCopy)
		sm.configs = append(sm.configs, smConfigCopy)
		msg.clientId = joinArgs.ClientId
		msg.requestIndex = joinArgs.RequestIndex
		fmt.Printf("finish join apply, me: %v, config: %v.\n",sm.me, sm.configs)

	case MoveArgs:
		fmt.Printf("start move apply, me: %v.\n", sm.me)
		moveArgs := raftApplyMsg.Command.(MoveArgs)
		destGid := moveArgs.GID
		movingShard := moveArgs.Shard
		smConfigCopy.Shards[movingShard] = destGid
		sm.configs = append(sm.configs, smConfigCopy)
		msg.clientId = moveArgs.ClientId
		msg.requestIndex = moveArgs.RequestIndex
		fmt.Printf("finish move apply, me: %v.\n", sm.me)

	case LeaveArgs:
		fmt.Printf("start leave apply, me: %v.\n", sm.me)
		leaveArgs := raftApplyMsg.Command.(LeaveArgs)
		for _, gid := range leaveArgs.GIDs{
			for shardIndex, gidInShard := range smConfigCopy.Shards{
				if gid == gidInShard{
					smConfigCopy.Shards[shardIndex] = 0
				}
			}
			delete(smConfigCopy.Groups, gid)
		}
		sm.balanceShardInGroup(&smConfigCopy)
		sm.configs = append(sm.configs, smConfigCopy)
		msg.clientId = leaveArgs.ClientId
		msg.requestIndex = leaveArgs.RequestIndex
		fmt.Printf("finish leave apply, me: %v.\n", sm.me)

	case QueryArgs:
		println("start query apply.")
		queryArgs := raftApplyMsg.Command.(QueryArgs)
		msg.clientId = queryArgs.ClientId
		msg.requestIndex = queryArgs.RequestIndex
		fmt.Printf("finish query apply, me: %v, config: %v.\n", sm.me, sm.configs)
		break

	default:
		log.Fatalf("Wrong request type.\n", )
	}
	if handler, isExist := sm.responseHandler[raftApplyMsg.Index]; isExist{
		fmt.Printf("in commandApply, handle exist.\n")
		handler <- msg
	}

}

func (sm *ShardMaster) balanceShardInGroup(config *Config){
	// 每个组最少负责 总片数/总组数 个 shard。
	minShardsInGroup := NShards / len(config.Groups)
	wanderShard := make([]int, 0)
	groupContainsState := make(map[int] []int)	// gid -> shards
	// gid 为 0 的副本组为无效副本组
	for gid, _ := range config.Groups{
		groupContainsState[gid] = make([]int, 0)
	}
	for shardIndex, gid := range config.Shards{
		if gid == 0{
			wanderShard = append(wanderShard, shardIndex)
		} else {
			groupContainsState[gid] = append(
				groupContainsState[gid],
				shardIndex)
		}
	}

	isBalance := func() bool{
		shardCount := 0
		for _, shards := range groupContainsState{
			if len(shards) < minShardsInGroup{
				return false
			}
			shardCount += len(shards)
		}
		return shardCount == NShards
	}

	for !isBalance(){
		for gid, shards := range groupContainsState{
			for len(shards) < minShardsInGroup && len(wanderShard) > 0 {
				groupContainsState[gid] = append(
					groupContainsState[gid],
					wanderShard[0])
				wanderShard = wanderShard[1:]
			}

			if len(shards) < minShardsInGroup{
				for adjustGid, adJustShards := range groupContainsState{
					if len(adJustShards) > minShardsInGroup{
						groupContainsState[gid] = append(
							groupContainsState[gid],
							groupContainsState[adjustGid][0])
						groupContainsState[adjustGid] = groupContainsState[adjustGid][1:]
						break
					}
				}
			}
		}

		for gid, _ := range groupContainsState{
			if len(wanderShard) > 0 {
				groupContainsState[gid] = append(
					groupContainsState[gid],
					wanderShard[0])
				wanderShard = wanderShard[1:]
			}
		}

	}
	for gid, shards := range groupContainsState{
		for _, shardNumber := range shards{
			config.Shards[shardNumber] = gid
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.raftApplyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.raftApplyCh)
	sm.responseHandler = make(map[int]chan shardMasterApplyMsg)
	sm.lastRequest = make(map[int64] int)
	sm.forKill = make(chan struct{})

	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	go sm.run()

	return sm
}