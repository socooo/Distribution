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
	applyChan chan ApplyMsg
	forKill chan struct{}

	// PersistState
	currentTerm int
	votedFor int
	logEntries []LogEntry

	// VolatileState
	commitIndex int		// 已知已提交的最高日志条目的索引.
	lastApplied int		// 应用于状态机的最高日志条目的索引.
	logIndex int 		// 已知最高日志条目的索引.

	// LeaderVolatileState
	nextIndex[]	int		// 对于每个服务器，要发送到该服务器的下一个日志条目的索引.
	matchIndex[] int	// 对于每个服务器，已知在服务器上复制的最高日志条目的索引.
}
type LogEntry struct{
	LogIndex int // start from 1
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
}
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int	// 候选人最后一个日志条目的索引.
	LastLogTerm int		// 候选人最后一个日志条目的 term.
}
type RequestVoteReply struct {
	CandidateId int
	Term int
	VoteGranted bool
	SendOk bool
	Destination int
}
type serverState int
const heartBeatTime = time.Duration(120 * time.Millisecond)
const(
	leader serverState = iota
	candidate
	follower
)

func genVoteTimeOut(serverno int, funName string) int{
	voteTimeout := rand.Intn(200) + 500
	//fmt.Printf("server no: %v, in function: %v, gen vote time out: %v\n", serverno, funName, voteTimeout)
	return voteTimeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
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

func (rf *Raft) launchElection() bool{
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.mu.Unlock()
	//fmt.Printf("server: %v launch term: %v\n", rf.me, rf.currentTerm)
	serverNum, voteCount := len(rf.peers), 0
	var majorityNum int
	if serverNum % 2 == 0{
		majorityNum = serverNum/2
	}else {majorityNum = serverNum/2 + 1}
	voteArgs := RequestVoteArgs{
		Term:rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm: rf.currentTerm}
	voteChan := make(chan RequestVoteReply)

	for i:=0; i < serverNum; i++{
		go func(j int, args RequestVoteArgs) {
			voteReply := RequestVoteReply{
				Term:-1,
				VoteGranted:false}
			//fmt.Printf("in launch election, server: %v send voteRequest to : %v, term: %v, reply content: %v.\n", rf.me, j, rf.currentTerm, reply)
			if rf.sendRequestVote(j, &voteArgs, &voteReply){
				//fmt.Printf("in launch election for term: %v, server: %v, return content: %v.\n", rf.currentTerm, rf.me, voteReply)
				voteChan <- voteReply
			} else {
				voteChan <- RequestVoteReply{SendOk:false, Destination:j}
			}
		}(i, voteArgs)
	}
	voteReceiveTimer := time.NewTimer(time.Duration(300)*time.Millisecond)
	for voteCount < majorityNum {
		select {
			case eleResult := <- voteChan:
				// fmt.Printf("in launch election, voteResult: %v.\n", eleResult)
				if !eleResult.SendOk{
					destination := eleResult.Destination
					go func(destination int, args RequestVoteArgs) {
						voteReply := RequestVoteReply{
							Term:-1,
							VoteGranted:false}
						if rf.sendRequestVote(destination, &voteArgs, &voteReply){
							voteChan <- voteReply
						} else {
							voteChan <- RequestVoteReply{SendOk:false, Destination: destination}
						}
					}(destination, voteArgs)
				} else if eleResult.VoteGranted && eleResult.Term == rf.currentTerm{
					voteCount += 1
				}
			case <- voteReceiveTimer.C:
				//fmt.Printf("no Leader in term: %v\n", rf.currentTerm)
				return false
		}
	}
	if voteCount >= majorityNum {
		//fmt.Printf("receive enough vote: %v, majority is: %v, term: %v.\n", voteCount, majorityNum, rf.currentTerm)
		rf.mu.Lock()
		rf.state = leader
		for i := 0; i < serverNum; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.commitIndex + 1
			// fmt.Printf("vote done, matchIndex for %v: %v.\n", i, rf.matchIndex[i])
		}
		rf.mu.Unlock()
		return true
	}
	return false
}

func (rf *Raft) Apply(indexOfApply int) (newLastApplied int){
	oldLastApplied := rf.lastApplied
	for j := oldLastApplied + 1; j <= indexOfApply; j++ {
		//fmt.Printf("in Apply server: %v, is applying index: %v, content: %v.\n", rf.me, j, rf.logEntries[j])
		newApplyMsg := ApplyMsg{Index: j,
			Command: rf.logEntries[j].Command}
		rf.mu.Lock()
		rf.applyChan <- newApplyMsg
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.lastApplied = indexOfApply
	rf.commitIndex = indexOfApply
	rf.mu.Unlock()

	newLastApplied = rf.lastApplied
	return
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.state = follower
	close(rf.forKill)
}

func (rf *Raft) AppendSend(){
	serverNum := len(rf.peers)
	if rf.state != leader{
		rf.electionTimer.Stop()
		rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendSend")) * time.Millisecond)
		return
	}
	for i:=0; i < serverNum; i++ {
		if i==rf.me{continue}
		go func(serverNo int) {
			sendFirst := rf.nextIndex[serverNo]
			sendFinal := rf.logIndex + 1
			appendArg := rf.constructAppendEntriesArgs(serverNo, sendFirst, sendFinal)
			appendReply := AppendEntriesReply{}
			sendAppendTimer := time.NewTimer(75 * time.Millisecond)
			//fmt.Printf("in append send, append start, append entries content: %v, leader's entries: %v, sendfirst: %v, send final: %v.\n", appendArg.Entries, rf.logEntries, sendFirst, sendFinal)
			for{
				select{
				case <- sendAppendTimer.C:
					//fmt.Printf("term: %v, send append to: %v time up.\n", rf.currentTerm, serverNo)
					return
				default:
					if rf.sendAppendEntries(serverNo, &appendArg, &appendReply){
						//fmt.Printf("send done. reply content, term: %v, success: %v\n", appendReply.Term, appendReply.Success)
						if !appendReply.Success{
							if appendReply.Term > rf.currentTerm{
								rf.mu.Lock()
								rf.state = follower
								rf.currentTerm = appendReply.Term
								rf.mu.Unlock()
								return
							} else if rf.state == follower{
								return
							}else{
								//fmt.Printf("in appendSend, reply term: %v, current term: %v, -1 retry.\n", appendReply.Term, rf.currentTerm)
								sendFirst = appendReply.ConflictIndex
								rf.mu.Lock()
								rf.nextIndex[serverNo] = sendFirst + 1
								rf.mu.Unlock()
								appendArg = rf.constructAppendEntriesArgs(serverNo, sendFirst, sendFinal)
							}
						}else{
							rf.mu.Lock()
							rf.matchIndex[serverNo] = sendFinal - 1
							rf.nextIndex[serverNo] = sendFinal
							rf.mu.Unlock()
							//fmt.Printf("send to server: %v done, return.\n", serverNo)
							checkApplyNo := rf.logIndex
							commitCount := 0
							majorityNum := len(rf.peers)/2
							if checkApplyNo > rf.commitIndex{
								for k := 0; k < serverNum; k++ {
									if k == rf.me{continue}
									if checkApplyNo == rf.matchIndex[k]{
										commitCount += 1
									}
								}
								if commitCount >= majorityNum{
									go rf.Apply(checkApplyNo)
								}
							}
							return
						}
					}else {
						// println("send return false, return.")
						return
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) constructAppendEntriesArgs(serverNo int, sendFirst int, sendFinal int) (appendArg AppendEntriesArgs){
	//fmt.Printf("======================\nin constructAppendEntriesArgs:\n" +
	//	"\tterm: %v\n" +
	//	"\tleader: %v\n" +
	//	"\tsendfirst: %v\n" +
	//	"\tsendFinal: %v\n" +
	//	"\tcommitIndex: %v\n" +
	//	"\tprevLogIndex: %v\n" +
	//	"\tthis server no: %v\n======================\n",
	//	rf.currentTerm,
	//	rf.votedFor,
	//	sendFirst,
	//	sendFinal,
	//	rf.commitIndex,
	//	rf.matchIndex[serverNo],
	//	rf.me)
	//fmt.Printf("in construct, to: %v, sendFirst: %v, sendFinal: %v, matchIndex: %v, commitIndex: %v, term: %v, this server: %v.\n", serverNo, sendFirst, sendFinal, rf.matchIndex[serverNo], rf.commitIndex, rf.currentTerm, rf.me)
	if sendFirst >= sendFinal {
		appendArg = AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: sendFirst - 1,
			PrevLogTerm: rf.logEntries[sendFirst - 1].Term,
			Entries: nil,
			LeaderCommit: rf.commitIndex}

	}else {
		entries := rf.logEntries[sendFirst:sendFinal]

		appendArg = AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: sendFirst - 1,
			PrevLogTerm: rf.logEntries[sendFirst - 1].Term,
			Entries: entries,
			LeaderCommit: rf.commitIndex}

		//fmt.Printf("in construct, done, server: %v, content: %v.\n",rf.me, appendArg)
	}
	return
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
	newLogEntry := LogEntry{
		LogIndex: rf.logIndex + 1,
		Term: rf.currentTerm,
		Command: command}
	rf.mu.Lock()
	rf.logEntries = append(rf.logEntries, newLogEntry)
	rf.logIndex = rf.logIndex + 1
	rf.mu.Unlock()
	index := rf.logIndex
	term := rf.currentTerm
	isLeader := true
	fmt.Printf("in Start, me: %v, newLogEntry: %v.\n", rf.me, newLogEntry)
	return index, term, isLeader
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
	serverNum := len(peers)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.state = follower
	rf.matchIndex = make([]int, serverNum)
	rf.nextIndex = make([]int, serverNum)
	rf.applyChan = applyCh
	rf.forKill = make(chan struct{})
	initLogEntry := LogEntry{LogIndex: 0, Term: 0, Command:"init"}
	rf.logEntries = append(rf.logEntries, initLogEntry)
	rf.logIndex = 0
	//fmt.Printf("server: %v, init done.\n",me)

	go func(myNum int) {
		rf.electionTimer = time.NewTimer(time.Duration(genVoteTimeOut(rf.me, "Make goroutine init")) * time.Millisecond)
		for{
			select {
			case <- rf.electionTimer.C:
				electionResult := rf.launchElection()
				if electionResult{
					heartBeatTimer := time.NewTicker(heartBeatTime)
					for{
						select {
						case <- heartBeatTimer.C:
							if rf.state == leader{
								go rf.AppendSend()
							} else {
								rf.electionTimer.Stop()
								rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "Make goroutine election failure")) * time.Millisecond)
								break
							}
						case <- rf.forKill:
							fmt.Printf("---kill in heart, server: %v---\n", rf.me)
							return
						}
					}
				} else {
					rf.electionTimer.Stop()
					rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "Make goroutine election failure")) * time.Millisecond)
				}
				case <- rf.forKill:
					fmt.Printf("---kill in Make, server: %v---\n", rf.me)
					return
			}
		}
	}(rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}