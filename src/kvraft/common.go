package raftkv

const (
	OK Err      = "OK"
	ErrNoKey = "ErrNoKey"
	WrongLeader = "WrongLeader"
	TimeOut = "ApplyTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RequestIndex int
	ClientId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	RequestIndex int
	ClientId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
type opType string

const(
	Put opType = "put"
	Append = "append"
	Get = "get"
)

type kvApplyMsg struct {
	value string
	err Err
}