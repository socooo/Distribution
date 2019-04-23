package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me int                 // this peer's index into peers[]
	state serverState
	electionTimer *time.Timer

	// PersistState
	currentTerm int
	votedFor int
	log []LogEntry

	// VolatileState
	commitIndex int
	lastApplied int

	// LeaderVolatileState
	nextIndex[]	int
	matchIndex[] int
}
type LogEntry struct{
	Term int
	Command interface{}
}
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
}
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}
type serverState int
const heartBeatTime = time.Duration(120 * time.Millisecond)
const(
	leader serverState = iota
	candidate
	follower
)

func genVoteTimeOut(serverno int, funName string) int{
	voteTimeout := rand.Intn(250) + 600
	//fmt.Printf("server no: %v, in function: %v, gen vote time out: %v\n", serverno, funName, voteTimeout)
	return voteTimeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	isLeader = rf.votedFor == rf.me
	term = rf.currentTerm
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "RequestVote"))*time.Millisecond)
	rf.mu.Unlock()
	if args.CandidateId == rf.me{
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else if rf.currentTerm >= args.Term ||
		args.LastLogTerm < rf.currentTerm ||
		args.LastLogIndex < rf.commitIndex{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if rf.state == candidate && args.CandidateId != rf.me{
		if args.Term > rf.currentTerm{
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.state = follower
			rf.currentTerm = args.Term
			rf.mu.Unlock()
			reply.Term = rf.currentTerm

		} else{
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else {
		reply.VoteGranted = true
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.Term = rf.currentTerm

	}
	fmt.Printf("servernum: %v, before request vote return, reply content, term:%v, vote granted:%v\n", rf.me, reply.Term, reply.VoteGranted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendEntries"))*time.Millisecond)
	rf.mu.Unlock()
	//fmt.Printf("In append entries, leaderId: %v, this serverNo: %v, args term: %v, current term: %v\n", args.LeaderId, rf.Me, args.Term, rf.CurrentTerm)
	if args.Term < rf.currentTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
	}else{
		if rf.state == leader && rf.currentTerm < args.Term{
			rf.state = follower
		}
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.mu.Unlock()
		reply.Term = args.Term
		reply.Success = true
	}
	//fmt.Printf("append result before return: %v\n", reply.Success)
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Printf("\nserver: %v, send request vote to %v\n",rf.Me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//fmt.Printf("vote reply content, term: %v, result: %v\n",reply.Term, reply.VoteGranted)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Printf("\nserver: %v, send append entries to %v\n",rf.Me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("append entries reply content, term: %v, result: %v\n",reply.Term, reply.Success)
	return ok
}

func (rf *Raft) launchElection() bool{
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm += 1
	rf.mu.Unlock()
	fmt.Printf("launch term: %v\n", rf.currentTerm)
	serverNum, voteCount := len(rf.peers), 0
	var majorityNum int
	if serverNum % 2 == 0{
		majorityNum = serverNum/2
	}else {majorityNum = serverNum/2 + 1}
	voteArgs := RequestVoteArgs{
		Term:rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.currentTerm,
		LastLogTerm: rf.commitIndex}
	voteReply := RequestVoteReply{
		Term:-1,
		VoteGranted:false}
	voteChan := make(chan bool)

	for i:=0; i < serverNum; i++{
		go func(j int, args RequestVoteArgs, reply RequestVoteReply) {
			if rf.sendRequestVote(j, &voteArgs, &voteReply){
				voteChan <- voteReply.VoteGranted
			} else {
				voteChan <- false
			}
		}(i, voteArgs, voteReply)
	}
	voteReceiveTimer := time.NewTimer(time.Duration(500)*time.Millisecond)
	for voteCount < majorityNum {
		select {
			case eleResult := <- voteChan:
				if eleResult{
					voteCount += 1
				}
			case <- voteReceiveTimer.C:
				return false
		}
	}

	if voteCount >= majorityNum {
		rf.mu.Lock()
		rf.state = leader
		rf.mu.Unlock()
		return true
	}
	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != leader{
		return -1, -1 ,false
	}
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state == leader{
		isLeader = true
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}
func (rf *Raft) AppendSend(){
	serverNum := len(rf.peers)
	for{
		if rf.state != leader{
			rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendSend")) * time.Millisecond)
			break
		}
		heartBeatArg := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.lastApplied,
			PrevLogTerm: rf.currentTerm,
			Entries: nil,
			LeaderCommit: rf.commitIndex}

		heartBeatReply := AppendEntriesReply{}
		heartGroup := sync.WaitGroup{}
		heartGroup.Add(serverNum)
		for i:=0; i < serverNum; i++ {
			go func(serverNo int, group *sync.WaitGroup) {
				defer group.Done()
				rf.sendAppendEntries(serverNo, &heartBeatArg, &heartBeatReply)
				if !heartBeatReply.Success{
					if rf.currentTerm < heartBeatReply.Term{
						rf.currentTerm = heartBeatReply.Term
						rf.state = follower
					}
				}
			}(i, &heartGroup)
		}
		heartGroup.Wait()
		//fmt.Printf("Append send once done.\n")
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.state = follower

	fmt.Printf("server: %v, init done.\n",me)

	go func(myNum int) {
		rf.electionTimer = time.NewTimer(time.Duration(genVoteTimeOut(rf.me, "Make goroutine init")) * time.Millisecond)
		for{
			select {
			case <- rf.electionTimer.C:
				fmt.Printf("server: %v, election time out\n",myNum)
				electionResult := rf.launchElection()
				if electionResult{
					heartBeatTimer := time.NewTicker(heartBeatTime)
					for{
						select {
						case <- heartBeatTimer.C:
							go rf.AppendSend()
						}
					}
				} else {
					rf.electionTimer.Stop()
					rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "Make goroutine eletion failure")) * time.Millisecond)
				}
			}
		}
	}(rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
