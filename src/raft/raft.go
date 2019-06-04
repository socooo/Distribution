package raft

import (
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
	"log"
	"fmt"
)

func genVoteTimeOut(serverno int, funName string) int{
	voteTimeout := rand.Intn(200) + 300
	DPrintf("server no: %v, in function: %v, gen vote time out: %v\n", serverno, funName, voteTimeout)
	return voteTimeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	isLeader = rf.VotedFor == rf.me
	term = rf.CurrentTerm
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LogEntries)
	data := w.Bytes()
	DPrintf("in persist, me: %v, persist content: \n" +
		"\tterm: %v\n" +
		"\tvoteFor: %v\n" +
		"\tcommitIndex: %v\n" +
		"\tlog: %v.\n", rf.me, rf.CurrentTerm,rf.VotedFor,rf.CommitIndex,rf.LogEntries)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()

	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(r)
	currentTerm, votedFor, commitIndex := 0, 0, 0
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&rf.LogEntries) != nil{
		log.Fatal("error occur in read persist.")
	}
	rf.CurrentTerm, rf.VotedFor, rf.CommitIndex = currentTerm, votedFor, commitIndex
	DPrintf("in read persist, me: %v, persist content: \n" +
		"\tterm: %v\n" +
		"\tvoteFor: %v\n" +
		"\tcommitIndex: %v\n" +
		"\tlog: %v.\n", rf.me, rf.CurrentTerm,rf.VotedFor,rf.CommitIndex,rf.LogEntries)
}

func (rf *Raft) launchElection() bool{
	rf.mu.Lock()
	rf.state = candidate
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.mu.Unlock()
	lastLogIndex := len(rf.LogEntries) - 1
	fmt.Printf("server: %v launch term: %v\n", rf.me, rf.CurrentTerm)
	serverNum, voteCount := len(rf.peers), 0
	var majorityNum int
	if serverNum % 2 == 0{
		majorityNum = serverNum/2
	}else {majorityNum = serverNum/2 + 1}
	voteArgs := RequestVoteArgs{
		Term:rf.CurrentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: rf.LogEntries[lastLogIndex].Term,
		LastCommit: rf.CommitIndex}
	voteChan := make(chan RequestVoteReply)

	for i:=0; i < serverNum; i++{
		go func(j int, args RequestVoteArgs) {
			voteReply := RequestVoteReply{
				Term:-1,
				VoteGranted:false}
			//fmt.Printf("in launch election, server: %v send voteRequest to : %v, term: %v, reply content: %v.\n", rf.me, j, rf.CurrentTerm, reply)
			if rf.sendRequestVote(j, &voteArgs, &voteReply){
				fmt.Printf("in launch election for term: %v, server: %v, return content: %v.\n", rf.CurrentTerm, rf.me, voteReply)
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
				} else if eleResult.VoteGranted && eleResult.Term == rf.CurrentTerm{
					voteCount += 1
				}
			case <- voteReceiveTimer.C:
				//fmt.Printf("no Leader in term: %v\n", rf.CurrentTerm)
				return false
		}
	}
	if voteCount >= majorityNum {
		fmt.Printf("receive enough vote: %v, majority is: %v, term: %v.\n", voteCount, majorityNum, rf.CurrentTerm)
		rf.mu.Lock()
		rf.state = leader
		for i := 0; i < serverNum; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.CommitIndex + 1
			// fmt.Printf("vote done, matchIndex for %v: %v.\n", i, rf.matchIndex[i])
		}
		rf.mu.Unlock()
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) Apply(indexOfApply int){
	// fmt.Printf("in Apply server: %v, apply index: %v, last index: %v, last applied: %v.\n", rf.me, indexOfApply, len(rf.LogEntries)-1, rf.lastApplied)
	rf.mu.Lock()
	oldLastApplied := rf.lastApplied
	if indexOfApply > (len(rf.LogEntries)-1) {
		return
	}
	if indexOfApply > rf.lastApplied{
		//fmt.Printf("in Apply server: %v, apply index: %v, last index: %v, last applied: %v.\n", rf.me, indexOfApply, len(rf.LogEntries)-1, rf.lastApplied)
		for j := oldLastApplied + 1; j <= indexOfApply; j++ {
			newApplyMsg := ApplyMsg{Index: j,
				Command: rf.LogEntries[j].Command, me: rf.me}

			rf.lastApplied = j
			rf.CommitIndex = j
			//fmt.Printf("in Apply, before send. server: %v, commit index: %v, channel len: %v.\n", rf.me, rf.CommitIndex, len(rf.applyChan))
			rf.applyChan <- newApplyMsg
		}
		fmt.Printf("Apply done. server: %v, commit index: %v.\n", rf.me, rf.CommitIndex)
		rf.persist()
	}
	rf.mu.Unlock()

	return
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.state = follower
	close(rf.forKill)
}

func (rf *Raft) AppendSend(){
	if rf.state != leader{
		rf.electionTimer.Stop()
		rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendSend")) * time.Millisecond)
		return
	}
	lastLogIndex := len(rf.LogEntries) - 1
	checkApplyNo := lastLogIndex
	sendFinal := lastLogIndex + 1
	serverNum := len(rf.peers)
	rf.mu.RLock()
	checkApplyTerm := rf.LogEntries[checkApplyNo].Term
	rf.mu.RUnlock()
	currentCommit := rf.CommitIndex
	currentTerm := rf.CurrentTerm

	for i:=0; i < serverNum; i++ {
		if i==rf.me{continue}
		go func(serverNo int) {
			rf.mu.RLock()
			sendFirst := rf.nextIndex[serverNo]
			rf.mu.RUnlock()
			appendArg := rf.constructAppendEntriesArgs(serverNo, sendFirst, sendFinal, currentCommit, currentTerm)
			appendReply := AppendEntriesReply{}
			sendAppendTimer := time.NewTimer(75 * time.Millisecond)
			for{
				select{
				case <- sendAppendTimer.C:
					//fmt.Printf("term: %v, send append to: %v time up.\n", rf.CurrentTerm, serverNo)
					return
				default:
					if rf.sendAppendEntries(serverNo, &appendArg, &appendReply){
						//fmt.Printf("send done. reply content, term: %v, success: %v\n", appendReply.Term, appendReply.Success)
						if !appendReply.Success{
							if appendReply.Term > rf.CurrentTerm{
								rf.mu.Lock()
								rf.state = follower
								rf.CurrentTerm = appendReply.Term
								rf.mu.Unlock()
								rf.persist()
								return
							} else if rf.state == follower{
								return
							}else{
								//fmt.Printf("in appendSend, reply term: %v, current term: %v, -1 retry.\n", appendReply.Term, rf.CurrentTerm)
								sendFirst = appendReply.ConflictIndex
								rf.mu.Lock()
								rf.nextIndex[serverNo] = sendFirst + 1
								rf.mu.Unlock()
								appendArg = rf.constructAppendEntriesArgs(serverNo, sendFirst, sendFinal, currentCommit, currentTerm)
							}
						}else{
							rf.mu.Lock()
							if appendReply.MatchIndex > rf.matchIndex[serverNo]{
								rf.matchIndex[serverNo] = appendReply.MatchIndex
							}
							rf.nextIndex[serverNo] = sendFinal
							rf.mu.Unlock()
							//fmt.Printf("send to server: %v done, return.\n", serverNo)
							commitCount := 0
							majorityNum := len(rf.peers)/2
							if checkApplyNo > rf.CommitIndex && checkApplyTerm == rf.CurrentTerm {
								for k := 0; k < serverNum; k++ {
									if k == rf.me{continue}
									if checkApplyNo <= rf.matchIndex[k]{
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

func (rf *Raft) constructAppendEntriesArgs(serverNo int, sendFirst int, sendFinal int, currentCommit int, currentTerm int) (appendArg AppendEntriesArgs){
	// fmt.Printf("in construct, to: %v, sendFirst: %v, sendFinal: %v, matchIndex: %v, logIndex: %v, CommitIndex: %v, term: %v, this server: %v.\n", serverNo, sendFirst, sendFinal, rf.matchIndex[serverNo], len(rf.LogEntries) - 1, rf.CommitIndex, rf.CurrentTerm, rf.me)
	if sendFirst >= sendFinal {
		appendArg = AppendEntriesArgs{
			Term: currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: sendFinal - 1,
			PrevLogTerm: rf.LogEntries[sendFinal - 1].Term,
			Entries: nil,
			LeaderCommit: currentCommit}

	} else {
		//fmt.Printf("in construct, start send, server: %v, first: %v, final: %v.\n",rf.me, sendFirst, sendFinal)
		entries := rf.LogEntries[sendFirst: sendFinal]

		appendArg = AppendEntriesArgs{
			Term: currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: sendFirst - 1,
			PrevLogTerm: rf.LogEntries[sendFirst - 1].Term,
			Entries: entries,
			LeaderCommit: currentCommit}

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
	rf.mu.Lock()
	newLogIndex := len(rf.LogEntries)
	newLogEntry := LogEntry{
		LogIndex: newLogIndex,
		Term: rf.CurrentTerm,
		Command: command}
	rf.LogEntries = append(rf.LogEntries, newLogEntry)
	rf.mu.Unlock()
	index := newLogIndex
	term := rf.CurrentTerm
	isLeader := true
	rf.persist()
	rf.mu.Lock()
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(heartBeatTime)
	rf.mu.Unlock()
	go rf.AppendSend()
	fmt.Printf("in Start, me: %v, newLogEntry: %v.\n", rf.me, newLogEntry)
	return index, term, isLeader
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	serverNum := len(peers)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.VotedFor = -1
	rf.lastApplied = 0
	rf.state = follower
	rf.matchIndex = make([]int, serverNum)
	rf.nextIndex = make([]int, serverNum)
	rf.applyChan = applyCh
	rf.forKill = make(chan struct{})
	initLogEntry := LogEntry{LogIndex: 0, Term: 0, Command:"init"}
	rf.LogEntries = append(rf.LogEntries, initLogEntry)
	// initialize from state persisted before a crash
	rf.readPersist()

	//fmt.Printf("server: %v, init done.\n",me)

	go func(myNum int) {
		rf.electionTimer = time.NewTimer(time.Duration(genVoteTimeOut(rf.me, "Make goroutine init")) * time.Millisecond)
		for{
			select {
			case <- rf.electionTimer.C:
				electionResult := rf.launchElection()
				if electionResult{
					rf.heartBeatTimer = time.NewTimer(heartBeatTime)
					for{
						select {
						case <- rf.heartBeatTimer.C:
							if rf.state == leader{
								rf.mu.Lock()
								rf.heartBeatTimer.Stop()
								rf.heartBeatTimer.Reset(heartBeatTime)
								rf.mu.Unlock()
								go rf.AppendSend()
							} else {
								rf.mu.Lock()
								rf.electionTimer.Stop()
								rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "Make goroutine step down")) * time.Millisecond)
								rf.mu.Unlock()
								break
							}
						case <- rf.forKill:
							return
						}
					}
				} else {
					rf.mu.Lock()
					rf.state = follower
					rf.electionTimer.Stop()
					fmt.Printf("launch return false, reset double time.\n")
					rf.electionTimer.Reset(time.Duration(2 * genVoteTimeOut(rf.me, "Make goroutine election failure")) * time.Millisecond)
					rf.mu.Unlock()
				}
				case <- rf.forKill:
					return
			}
		}
	}(rf.me)
	return rf
}