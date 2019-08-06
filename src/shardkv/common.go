package shardkv

import (
	"sync"
	"time"
	"raft"
	"labrpc"
	"shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	raftApplyCh chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	data map[int] map[string]string // shardIndex -> data
	responseHandler map[int] chan shardApplyMsg
	lastRequest map[int64] int
	forKill chan struct{}
	mck *shardmaster.Clerk
	lastConfig shardmaster.Config
	shardsState [shardmaster.NShards]ShardState	// shardIndex -> ShardState
}

const Debug = 0
const ShardApplyTimeOut = 1 * time.Second
const PollingInterval = 100 * time.Millisecond

const (
	OK Err      = "OK"
	ErrNoKey = "ErrNoKey"
	WrongLeader = "WrongLeader"
	TimeOut = "ApplyTimeOut"
	ErrWrongGroup = "ErrWrongGroup"
	MigratingData = "MigratingData"
)
type Err string

const (
	Available ShardState = "available"
	DataExporting = "dataExporting"
	DataImporting = "dataImporting"
	NotAvailable = "notAvailable"
)

type ShardState string

type RaftKvData struct {
	Data map[int]map[string]string
	LastRequest map[int64] int
	LastConfig shardmaster.Config
	ShardState [shardmaster.NShards]ShardState
}

type DataOp struct {
	RequestType opType
	Key string
	Value string
	RequestIndex int
	ClientId int64
}

type ConfigOp struct {
	ShardsConfig shardmaster.Config
}

type ShardOp struct {
	RequestType opType
	Data map[int]map[string]string	// shardIndex -> data
	Config shardmaster.Config
	Shards []int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RequestIndex int
	ClientId int64
}

type PutAppendReply struct {
	Err         Err
}

type GetArgs struct {
	Key string
	RequestIndex int
	ClientId int64
}

type GetReply struct {
	Err         Err
	Value       string
}
type opType string

const(
	Put opType = "put"
	Append = "append"
	Get = "get"
	SendShards = "sendShards"
	CleanShards = "cleanShards"
)

type shardApplyMsg struct {
	value string
	err Err
	requestIndex int
	clientId int64
}

type MigratingArgs struct {
	Data map[int]map[string]string // shardIndex -> data
	Config shardmaster.Config
	Shards []int
}

type MigratingReply struct {
	Err Err
}

type handleReply struct {
	err Err
	value string
}