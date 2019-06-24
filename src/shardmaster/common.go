package shardmaster

import (
	"sync"
	"time"
	"raft"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	raftApplyCh chan raft.ApplyMsg

	lastRequest map[int64] int	// clientId -> requestIndex
	responseHandler map[int]chan shardMasterApplyMsg
	forKill chan struct{}
	configs []Config // indexed by config num
}

const smApplyTimeout = time.Duration(2 * time.Second)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK Err = "OK"
	WrongLeader = "WrongLeader"
	TimeOut = "ApplyTimeOut"
	DuplicateRequest = "DuplicateRequest"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	RequestIndex int
	ClientId int64
}

type JoinReply struct {
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	RequestIndex int
	ClientId int64
}

type LeaveReply struct {
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	RequestIndex int
	ClientId int64
}

type MoveReply struct {
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	RequestIndex int
	ClientId int64
}

type QueryReply struct {
	Err         Err
	Config      Config
}

type shardMasterApplyMsg struct {
	requestIndex int
	clientId int64
}

type SmOpType int

const(
	Join SmOpType = iota
	Leave
	Move
	Query
)

type handleReply struct {
	err Err
}