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
	e.Encode(rf.maxLogIndex)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.LogEntries)
	data := w.Bytes()
	DPrintf("in persist, me: %v, persist content: \n" +
		"\tterm: %v\n" +
		"\tvoteFor: %v\n" +
		"\tcommitIndex: %v\n" +
		"\tlog: %v.\n", rf.me, rf.CurrentTerm,rf.VotedFor,rf.CommitIndex,rf.LogEntries)
	rf.Persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.Persister.ReadRaftState()
	fmt.Printf("read persist.\n")
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(r)
	currentTerm, votedFor, maxIndex, commitIndex, lastIncludeIndex, lastIncludeTerm, lastApplied :=
		0, 0, 0, 0, 0, 0, 0
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&maxIndex) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&lastIncludeIndex) != nil ||
		decoder.Decode(&lastIncludeTerm) != nil ||
		decoder.Decode(&lastApplied) != nil ||
		decoder.Decode(&rf.LogEntries) != nil{
		log.Fatal("error occur in read persist.\n")
	}
	rf.CurrentTerm, rf.VotedFor, rf.maxLogIndex, rf.CommitIndex, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.lastApplied =
		currentTerm, votedFor, maxIndex, commitIndex, lastIncludeIndex, lastIncludeTerm, lastApplied
	DPrintf("in read persist, me: %v, persist content: \n" +
		"\tterm: %v\n" +
		"\tvoteFor: %v\n" +
		"\tcommitIndex: %v\n" +
		"\tlog: %v.\n", rf.me, rf.CurrentTerm,rf.VotedFor,rf.CommitIndex,rf.LogEntries)
}

func (rf *Raft) launchElection() bool{
	rf.Mu.Lock()
	rf.state = candidate
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.Mu.Unlock()
	lastLogIndex := rf.maxLogIndex
	serverNum, voteCount := len(rf.peers), 0
	var majorityNum int
	var voteArgs RequestVoteArgs
	if serverNum % 2 == 0{
		majorityNum = serverNum/2
	}else {majorityNum = serverNum/2 + 1}
	if lastLogIndex == rf.lastIncludeIndex{
		fmt.Printf("server: %v launch term: %v\n", rf.me, rf.CurrentTerm)
		voteArgs = RequestVoteArgs{
			Term:rf.CurrentTerm,
			CandidateId: rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm: rf.lastIncludeTerm,
			LastCommit: rf.CommitIndex}
	} else {
		lastIndexInLog, ok := rf.findLogIndex(lastLogIndex)
		if !ok{
			log.Fatalf("index is not exist in launchElection, maxIndex: %v, logs: %v.\n", rf.maxLogIndex, rf.LogEntries)
		}
		fmt.Printf("server: %v launch term: %v\n", rf.me, rf.CurrentTerm)

		voteArgs = RequestVoteArgs{
			Term:rf.CurrentTerm,
			CandidateId: rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm: rf.LogEntries[lastIndexInLog].Term,
			LastCommit: rf.CommitIndex}
	}

	voteChan := make(chan RequestVoteReply)

	for i:=0; i < serverNum; i++{
		go func(j int, args RequestVoteArgs) {
			voteReply := RequestVoteReply{
				Term:-1,
				VoteGranted:false}
			//fmt.Printf("in launch election, server: %v send voteRequest to : %v, term: %v, reply content: %v.\n", rf.me, j, rf.CurrentTerm, reply)
			if rf.sendRequestVote(j, &voteArgs, &voteReply){
				// fmt.Printf("in launch election for term: %v, server: %v, return content: %v.\n", rf.CurrentTerm, rf.me, voteReply)
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
		// fmt.Printf("receive enough vote: %v, majority is: %v, term: %v.\n", voteCount, majorityNum, rf.CurrentTerm)
		rf.Mu.Lock()
		rf.state = leader
		for i := 0; i < serverNum; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.CommitIndex + 1
			// fmt.Printf("vote done, matchIndex for %v: %v.\n", i, rf.matchIndex[i])
		}
		rf.Mu.Unlock()
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) Apply(indexOfApply int, useSnapshot bool){
	// fmt.Printf("in Apply server: %v, apply index: %v, last index: %v, last applied: %v.\n", rf.me, indexOfApply, rf.maxLogIndex, rf.lastApplied)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if useSnapshot{
		msg := ApplyMsg{UseSnapshot: true, Snapshot: rf.Persister.ReadSnapshot()}
		rf.applyChan <- msg
		return
	} else {
		oldLastApplied := rf.lastApplied
		if indexOfApply > rf.maxLogIndex {
			return
		}
		if indexOfApply > rf.lastApplied{
			// fmt.Printf("in Apply server: %v, apply index: %v, last index: %v, last applied: %v.\n", rf.me, indexOfApply, rf.maxLogIndex, rf.lastApplied)
			for j := oldLastApplied + 1; j <= indexOfApply; j++ {
				jInLog, ok := rf.findLogIndex(j)
				if !ok{
					log.Fatalf("Index %v is not exist in Apply.\n", j)
				}
				newApplyMsg := ApplyMsg{Index: j,
					Command: rf.LogEntries[jInLog].Command, me: rf.me}

				rf.lastApplied = j
				rf.CommitIndex = j
				// fmt.Printf("in Apply, before send. server: %v, commit index: %v, channel len: %v.\n", rf.me, rf.CommitIndex, len(rf.applyChan))
				rf.applyChan <- newApplyMsg
			}
			// fmt.Printf("Apply done. server: %v, commit index: %v.\n", rf.me, rf.CommitIndex)
			rf.persist()
		}
		return
	}
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
	rf.Mu.Lock()
	lastLogIndex := rf.maxLogIndex
	checkApplyNo := lastLogIndex
	checkApplyNoInLog, ok := rf.findLogIndex(checkApplyNo)
	if !ok{
		log.Fatalf("Index %v is not exist in AppendSend.\n", checkApplyNo)
	}
	sendFinal := lastLogIndex + 1
	serverNum := len(rf.peers)
	checkApplyTerm := rf.LogEntries[checkApplyNoInLog].Term
	rf.Mu.Unlock()
	currentCommit := rf.CommitIndex
	currentTerm := rf.CurrentTerm

	for i:=0; i < serverNum; i++ {
		if i==rf.me{continue}
		go func(serverNo int) {
			rf.Mu.Lock()
			sendFirst := rf.nextIndex[serverNo]
			// 当 nextIndex 小于 主服务器的 lastIncludeIndex时，
			// 调用 InstallSnapshot 给落后的追随者发送快照。
			if sendFirst <= rf.lastIncludeIndex{
				snapArgs := InstallSnapshotArgs{
					Term: rf.CurrentTerm,
					LeaderId: rf.me,
					LastIncludedIndex: rf.lastIncludeIndex,
					LastIncludedTerm:rf.lastIncludeTerm,
					Data: rf.Persister.ReadSnapshot()}
				snapReply := InstallSnapshotReply{}

				if rf.sendSnapshot(serverNo, &snapArgs, &snapReply){
					if snapReply.Term > rf.CurrentTerm{
						rf.state = follower
						rf.CurrentTerm = snapReply.Term
						rf.persist()
					} else {
						rf.matchIndex[serverNo] = rf.lastIncludeIndex
						rf.nextIndex[serverNo] = rf.lastIncludeIndex + 1
						rf.persist()
						// fmt.Printf("in AppendSend, send snapshot ok, me:%v, to: %v, sendFirst: %v, lastInclude: %v.\n", rf.me, serverNo, sendFirst, rf.lastIncludeIndex)
					}
				}
				rf.Mu.Unlock()
				return
			}
			appendArg := rf.constructAppendEntriesArgs(serverNo, sendFirst, sendFinal, currentCommit, currentTerm)
			rf.Mu.Unlock()
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
							rf.Mu.Lock()
							if appendReply.Term > rf.CurrentTerm{
								rf.state = follower
								rf.CurrentTerm = appendReply.Term
								rf.Mu.Unlock()
								rf.persist()
								return
							} else if rf.state == follower{
								rf.Mu.Unlock()
								return
							} else if appendReply.ConflictIndex <= rf.lastIncludeIndex{
								snapArgs := InstallSnapshotArgs{
									Term: rf.CurrentTerm,
									LeaderId: rf.me,
									LastIncludedIndex: rf.lastIncludeIndex,
									LastIncludedTerm:rf.lastIncludeTerm,
									Data: rf.Persister.ReadSnapshot()}
								snapReply := InstallSnapshotReply{}

								if rf.sendSnapshot(serverNo, &snapArgs, &snapReply){
									if snapReply.Term > rf.CurrentTerm{
										rf.state = follower
										rf.CurrentTerm = snapReply.Term
										rf.persist()
									} else {
										rf.matchIndex[serverNo] = rf.lastIncludeIndex
										rf.nextIndex[serverNo] = rf.lastIncludeIndex + 1
										rf.persist()
										// fmt.Printf("in AppendSend, send snapshot in retry ok, me:%v, to: %v, conflict index: %v, last include: %v.\n", rf.me, serverNo, appendReply.ConflictIndex, rf.lastIncludeIndex)
									}
								}
								rf.Mu.Unlock()
								return
							} else {
								//fmt.Printf("in appendSend, reply term: %v, current term: %v, -1 retry.\n", appendReply.Term, rf.CurrentTerm)
								sendFirst = appendReply.ConflictIndex
								rf.nextIndex[serverNo] = sendFirst + 1
								appendArg = rf.constructAppendEntriesArgs(serverNo, sendFirst, sendFinal, currentCommit, currentTerm)
								rf.Mu.Unlock()
							}
						}else{
							rf.Mu.Lock()
							if appendReply.MatchIndex > rf.matchIndex[serverNo]{
								rf.matchIndex[serverNo] = appendReply.MatchIndex
							}
							rf.nextIndex[serverNo] = sendFinal
							rf.Mu.Unlock()
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
									go rf.Apply(checkApplyNo, false)
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
	//fmt.Printf("in construct, to: %v, sendFirst: %v, sendFinal: %v, matchIndex: %v, nextIndex: %v, logIndex: %v, CommitIndex: %v, term: %v, this server: %v.\n", serverNo, sendFirst, sendFinal, rf.matchIndex[serverNo], rf.nextIndex[serverNo], rf.maxLogIndex, rf.CommitIndex, rf.CurrentTerm, rf.me)
	if sendFirst >= sendFinal {
		if sendFinal - 1 <= rf.lastIncludeIndex{
			appendArg = AppendEntriesArgs{
				Term: currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.lastIncludeIndex,
				PrevLogTerm: rf.lastIncludeTerm,
				Entries: nil,
				LeaderCommit: currentCommit}
		} else {
			sendFinalInLog, finalOk := rf.findLogIndex(sendFinal - 1)
			if !finalOk{
				log.Fatalf("Index sendFinal, first > final, index: %v is not exist in AppendEntries, me: %v, maxIndex:%v, len entry: %v, lastInclude: %v",
					sendFinal, rf.me, rf.maxLogIndex, len(rf.LogEntries), rf.lastIncludeIndex)
			}
			appendArg = AppendEntriesArgs{
				Term: currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: sendFinal - 1,
				PrevLogTerm: rf.LogEntries[sendFinalInLog].Term,
				Entries: nil,
				LeaderCommit: currentCommit}
		}
	} else {
		sendFirstInLog, firstOk := rf.findLogIndex(sendFirst)
		sendFinalInLog, finalOk := rf.findLogIndex(sendFinal - 1)
		if !firstOk{
			log.Fatalf("Index sendFirst %v is not exist in AppendEntries, me: %v, maxIndex:%v, len entry: %v, lastInclude: %v",
				sendFirst, rf.me, rf.maxLogIndex, len(rf.LogEntries), rf.lastIncludeIndex)
		}
		if !finalOk{
			log.Fatalf("Index sendFinal %v is not exist in AppendEntries, me: %v, maxIndex:%v, len entry: %v, lastInclude: %v",
				sendFinal, rf.me, rf.maxLogIndex, len(rf.LogEntries), rf.lastIncludeIndex)
		}

		entries := rf.LogEntries[sendFirstInLog: sendFinalInLog + 1]
		// fmt.Printf("in construct, start send, server: %v, to: %v, sendFirst: %v, sendFinal: %v, firstInLog: %v, finalInLog: %v.\n",rf.me, serverNo, sendFirst, sendFinal, sendFirstInLog, sendFinalInLog)

		if sendFirstInLog == 0{
			appendArg = AppendEntriesArgs{
				Term: currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.lastIncludeIndex,
				PrevLogTerm: rf.lastIncludeTerm,
				Entries: entries,
				LeaderCommit: currentCommit}
		} else {
			appendArg = AppendEntriesArgs{
				Term: currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: sendFirst - 1,
				PrevLogTerm: rf.LogEntries[sendFirstInLog - 1].Term,
				Entries: entries,
				LeaderCommit: currentCommit}
		}
		//fmt.Printf("in construct, done, server: %v, content: %v.\n",rf.me, appendArg)
	}
	return
}

func (rf *Raft) LogCompact(lastSnapshotIndex int){
	if lastSnapshotIndex <= rf.lastIncludeIndex{
		return
	}
	if rf.CommitIndex >= lastSnapshotIndex{
		findIndex, indexExist := rf.findLogIndex(lastSnapshotIndex)
		if indexExist{
			lastEntry := rf.LogEntries[findIndex]
			rf.lastIncludeIndex = lastEntry.LogIndex
			rf.lastIncludeTerm = lastEntry.Term
			rf.LogEntries = rf.LogEntries[findIndex + 1:]
			rf.persist()
		} else {
			log.Fatal("Fail to compact rf's log, reason: index is not exist.\n")
		}
	} else {
		log.Fatal(" Fail to compact rf,s log, reason: snapshot's index is bigger than commit index.\n")
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != leader{
		return -1, -1 ,false
	}
	rf.Mu.Lock()
	newLogIndex := rf.maxLogIndex + 1
	rf.maxLogIndex = rf.maxLogIndex + 1
	newLogEntry := LogEntry{
		LogIndex: newLogIndex,
		Term: rf.CurrentTerm,
		Command: command}
	rf.LogEntries = append(rf.LogEntries, newLogEntry)
	rf.Mu.Unlock()
	index := newLogIndex
	term := rf.CurrentTerm
	isLeader := true
	rf.persist()
	rf.Mu.Lock()
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(heartBeatTime)
	rf.Mu.Unlock()
	go rf.AppendSend()
	fmt.Printf("in Start, me: %v, newLogEntry: %v.\n", rf.me, newLogEntry)
	return index, term, isLeader
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	serverNum := len(peers)
	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.me = me
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.VotedFor = -1
	rf.lastApplied = 0
	rf.state = follower
	rf.maxLogIndex = 0
	rf.lastIncludeTerm = 0
	rf.lastIncludeIndex = 0
	rf.matchIndex = make([]int, serverNum)
	rf.nextIndex = make([]int, serverNum)
	rf.applyChan = applyCh
	rf.forKill = make(chan struct{})
	initLogEntry := LogEntry{LogIndex: 0, Term: 0, Command:"init"}
	rf.LogEntries = append(rf.LogEntries, initLogEntry)
	// initialize from state persisted before a crash
	rf.readPersist()
	rf.replay()

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
								rf.Mu.Lock()
								rf.heartBeatTimer.Stop()
								rf.heartBeatTimer.Reset(heartBeatTime)
								rf.Mu.Unlock()
								go rf.AppendSend()
							} else {
								rf.Mu.Lock()
								rf.electionTimer.Stop()
								rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "Make goroutine step down")) * time.Millisecond)
								rf.Mu.Unlock()
								break
							}
						case <- rf.forKill:
							return
						}
					}
				} else {
					rf.Mu.Lock()
					rf.state = follower
					rf.electionTimer.Stop()
					// fmt.Printf("launch return false, reset double time.\n")
					rf.electionTimer.Reset(time.Duration(2 * genVoteTimeOut(rf.me, "Make goroutine election failure")) * time.Millisecond)
					rf.Mu.Unlock()
				}
				case <- rf.forKill:
					return
			}
		}
	}(rf.me)
	return rf
}

func (rf *Raft) findLogIndex(logIndex int) (int, bool){
	entryExist := false
	index := -1
	for indexInLog, ele := range rf.LogEntries{
		if ele.LogIndex == logIndex{
			entryExist = true
			index = indexInLog
			break
		}
	}
	return index, entryExist
}

func (rf *Raft) replay(){
	println("start reply, fetching lock.")
	if rf.lastIncludeIndex != 0 && rf.lastIncludeIndex < rf.maxLogIndex{
		rf.Mu.Lock()
		rf.lastApplied = rf.lastIncludeIndex
		rf.CommitIndex = rf.lastIncludeIndex
		rf.Mu.Unlock()
		go rf.Apply(rf.maxLogIndex, false)
	} else if rf.lastIncludeIndex == 0 && rf.maxLogIndex > 0{
		println("max index > 0, replaying.\n")
		rf.Mu.Lock()
		rf.lastApplied = rf.lastIncludeIndex
		rf.CommitIndex = rf.lastIncludeIndex
		rf.Mu.Unlock()
		go rf.Apply(rf.maxLogIndex, false)
	}
}