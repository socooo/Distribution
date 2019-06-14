package raft

import (
	"time"
	"sync"
	"labrpc"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
	me int
}

type Raft struct {
	Mu sync.RWMutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me int                 // this peer's index into peers[]
	state serverState
	electionTimer *time.Timer
	heartBeatTimer *time.Timer
	applyChan chan ApplyMsg
	forKill chan struct{}

	// PersistState
	CurrentTerm int
	VotedFor int
	LogEntries []LogEntry

	// VolatileState
	CommitIndex int		// 已知已提交的最高日志条目的索引.
	lastApplied int		// 应用于状态机的最高日志条目的索引.

	// LeaderVolatileState
	nextIndex[]	int		// 对于每个服务器，要发送到该服务器的下一个日志条目的索引.
	matchIndex[] int	// 对于每个服务器，已知在服务器上复制的最高日志条目的索引.

	// For snapshot
	lastIncludeIndex int
	lastIncludeTerm int

	maxLogIndex int
}

type LogEntry struct{
	LogIndex int
	Term int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int	// 紧接在新条目之前的日志条目索引.
	PrevLogTerm int		// prevLogIndex 条目的 term.
	Entries []LogEntry
	LeaderCommit int	// 当大多数服务器都提交了新的 LogEntry 时更新此参数，
	// 在下次 heartBeat 从服务器根据这个参数调用 Apply() 函数，更新状态。
}

type AppendEntriesReply struct {
	ConflictIndex int
	Term int
	Success bool
	MatchIndex int
}

type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int	// 候选人最后一个日志条目的索引.
	LastLogTerm int		// 候选人最后一个日志条目的 term.
	LastCommit int
}

type RequestVoteReply struct {
	CandidateId int
	Term int
	VoteGranted bool
	SendOk bool
	Destination int
	Reason int
}

type InstallSnapshotArgs struct{
	Term int 				// 领导者的 term
	LeaderId int			// 便于关注者可以重定向客户端
	LastIncludedIndex int	// 快照将替换所有包含此索引及之前的条目
	LastIncludedTerm int	// lastIncludedIndex 的 term
	Data []byte				// 快照块的原始字节，从偏移量开始
}

type InstallSnapshotReply struct {
	Term int
}

type serverState int
const heartBeatTime = time.Duration(120 * time.Millisecond)
const(
	leader serverState = iota
	candidate
	follower
)