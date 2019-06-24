package raft

import (
	"time"
	"log"
	"fmt"
)

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft)sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("servernum: %v, before start request vote,ask for vote: %v, args: %v, my state: %v, current commit: %v, term: %v.\n", rf.me, args.CandidateId, args, rf.state, rf.CommitIndex, rf.CurrentTerm)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reason := 0
	reply.SendOk = true
	reply.Destination = rf.me
	reply.CandidateId = args.CandidateId
	lastLogIndex := rf.maxLogIndex
	var lastLogTerm int
	if lastLogIndex == rf.lastIncludeIndex{
		lastLogTerm = rf.lastIncludeTerm
	} else {
		lastIndexInLog, ok := rf.findLogIndex(lastLogIndex)
		if !ok{
			log.Fatalf("Index lastLogIndex %v is not exist in RequestVote, maxIndex: %v, logs: %v.\n", lastLogIndex, rf.maxLogIndex, rf.LogEntries)
		}
		lastLogTerm = rf.LogEntries[lastIndexInLog].Term
	}

	if args.Term > rf.CurrentTerm && rf.state == leader{
		rf.state = follower
		rf.CurrentTerm = args.Term
	}
	if args.CandidateId == rf.me{
		reply.VoteGranted = true
		rf.VotedFor = rf.me
		reply.Term = rf.CurrentTerm
	} else if rf.CurrentTerm >= args.Term{
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 1
	} else if args.LastCommit < rf.CommitIndex {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 3
	} else if args.LastLogTerm < lastLogTerm{
		if args.Term > rf.CurrentTerm {
			// 当一个服务器断开连接又重新连接后，可能会立即发起选举，此时他的term比其他服务器高，
			// 但是 CommitIndex 小于其他服务器，其他服务器需要同步他的 term ，否则 append 操作会一直无法完成。
			rf.CurrentTerm = args.Term
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 2
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex{
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 4
	} else if rf.state != follower && args.CandidateId != rf.me{
		if args.Term > rf.CurrentTerm{
			reply.VoteGranted = true
			rf.state = follower
			rf.CurrentTerm = args.Term
			reply.Term = rf.CurrentTerm
		} else{
			reply.VoteGranted = false
			reply.Term = rf.CurrentTerm
		}
		reason = 5
	} else {
		reply.VoteGranted = true
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateId
		reply.Term = rf.CurrentTerm
	}
	reply.Reason = reason
	rf.persist()
	if reply.VoteGranted{
		rf.electionTimer.Stop()
		rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "RequestVote"))*time.Millisecond)
	}
	fmt.Printf("servernum: %v, before request vote return,ask for vote: %v, reply content, term:%v, vote granted:%v, my commit index: %v,args: %v, last log: %v, reason:%v.\n", rf.me, args.CandidateId, reply.Term, reply.VoteGranted, rf.CommitIndex, args, rf.LogEntries[lastLogIndex], reason)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.Mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendEntries"))*time.Millisecond)
	lastLogIndex := rf.maxLogIndex
	// fmt.Printf("In append entries, send from: %v, this serverNo: %v,current term: %v, args: %v.\n", args.LeaderId, rf.me, rf.CurrentTerm, args)
	reply.MatchIndex = args.PrevLogIndex
	entries := args.Entries
	appendLen := len(entries)
	if args.Term < rf.CurrentTerm{
		//fmt.Printf("in append entries: this server: %v, leader no: %v args's term: %v, server's term: %v.\n", rf.me, rf.VotedFor, args.Term, rf.CurrentTerm)
		reply.Success = false
		reply.Term = rf.CurrentTerm
		println(6)
		rf.Mu.Unlock()
		return
	}else if args.PrevLogIndex > lastLogIndex {
		//fmt.Printf("in append entries, args.PrevLogIndex > lastLogIndex: this server: %v, leader no: %v, args.PrevLogIndex's: %v, lastLogIndex: %v.\n",
		//	rf.me, rf.VotedFor, args.PrevLogIndex, lastLogIndex)
		reply.ConflictIndex = rf.CommitIndex + 1
		reply.Success = false
		reply.Term = rf.CurrentTerm
		fmt.Printf("5")
		rf.Mu.Unlock()
		return
	} else {
		//prevLogIndexInLog, prevLogOk := rf.findLogIndex(args.PrevLogIndex)
		//if !prevLogOk{
		//	log.Fatalf("Index args.PrevLogIndex %v is not exist in AppendEntries.\n", args.PrevLogIndex)
		//}
		//if args.PrevLogTerm != rf.LogEntries[prevLogIndexInLog].Term{
		//	// 若此 server 为 leader，断线后，客户端继续发送了多个 LogEntries，
		//	// 重连后，如果在心跳时不检测最新的 log 的term，
		//	// 重新链接的老 leader 就会 Apply 这些在断开连接后客户端发送的LogEntries
		//	//fmt.Printf("in append entries: args.PrevLogIndex: %v, args.PrevLogTerm: %v, rf.LogEntries[prevLogIndexInLog].Term: %v.\n", args.PrevLogIndex,  args.PrevLogTerm, rf.LogEntries[prevLogIndexInLog].Term)
		//
		//	reply.ConflictIndex = rf.CommitIndex + 1
		//	reply.Success = false
		//	reply.Term = rf.CurrentTerm
		//	return
		//}
		if rf.state != follower && rf.CommitIndex <= args.LeaderCommit{
			// state 为 candidate 时接收到 AppendEntries RPC 说明落选，置为 follower；
			// state 为 leader 时收到 appendEntries RPC，可能是此服务器作为主服务器时断线，在新的主服务器选出后重连，
			// 此服务器的 CommitIndex 落后于发送 Append RPC 的服务器则表示此服务器时老的 Leader，重置为 follower 状态，
			// 此服务器的 CommitIndex 等于发送 Append RPC 的服务器，无法判断哪一个是老服务器，都置为 follower 重新选举。
			//fmt.Printf("this server: %v, state: %v, CommitIndex: %v, args: %v.\n", rf.me, rf.state, rf.CommitIndex, args)
			rf.state = follower
			rf.persist()
		}

		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderId

		//fmt.Printf("\nin append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, receive entries content: %v.\n", rf.me, rf.VotedFor, args.PrevLogIndex, len(rf.LogEntries), entries)
		if entries != nil{
			appendStart := args.PrevLogIndex + 1
			if args.PrevLogIndex == 0{

				if rf.maxLogIndex > appendStart{
					rf.LogEntries = rf.LogEntries[0:1]
				}
				rf.LogEntries = append(rf.LogEntries, args.Entries...)
				rf.maxLogIndex = args.Entries[len(args.Entries) - 1].LogIndex
				// fmt.Printf("in append entries: this server: %v, leader no: %v, PrevIndex: %v, log len: %v, append init done, content: %v, maxIndex: %v.\n", rf.me, rf.VotedFor, args.PrevLogIndex, len(rf.LogEntries), rf.LogEntries, rf.maxLogIndex)
			}else if args.PrevLogIndex > rf.maxLogIndex{
				// 若此 server 断线重连后， CommitIndex 可能落后于主服务器很多条 LogEntries，需要重新发送这些 LogEntries。
				reply.ConflictIndex = rf.CommitIndex + 1
				reply.Success = false
				reply.Term = rf.CurrentTerm
				// fmt.Printf("in AE, conflict, this: %v, conflict: %v, leader: %v, args content: %v.\n", rf.me, reply.ConflictIndex, args.LeaderId, args)
				rf.Mu.Unlock()
				return
			} else {
				// fmt.Printf("in AE, start append, this: %v, leader: %v, args content: %v, log entries: %v.\n", rf.me, args.LeaderId, args, rf.LogEntries)
				if lastLogIndex >= appendStart{
					if appendStart <= rf.lastIncludeIndex{
						reply.ConflictIndex = rf.lastIncludeIndex + 1
						reply.Success = false
						reply.Term = rf.CurrentTerm
						rf.Mu.Unlock()
						return
					}
					lastIndexInLog, lastOk := rf.findLogIndex(lastLogIndex)
					if !lastOk{
						log.Fatalf("index lastLogIndex %v is not exist in AppendEntries, me: %v, maxIndex:%v, len entry: %v, lastInclude: %v",
							lastLogIndex, rf.me, rf.maxLogIndex, len(rf.LogEntries), rf.lastIncludeIndex)
					}
					appendStartInLog, appendStartOk := rf.findLogIndex(appendStart)
					if !appendStartOk{
						log.Fatalf("index appendStart %v is not exist in AppendEntries", appendStart)
					}
					conflictLoc := -1
					var compareLog = make([]LogEntry, lastLogIndex - args.PrevLogIndex)
					copy(compareLog, rf.LogEntries[appendStartInLog : lastIndexInLog + 1])

					lenCompareLog := len(compareLog)

					for i := 0; i < appendLen; i++ {
						if i < lenCompareLog {
							// fmt.Printf("in AE, this server: %v, compare: %v.\n", rf.me, compareIndex)
							if compareLog[i].Term != entries[i].Term{
								conflictLoc = i
								break
							}
						}else{break}
					}
					if conflictLoc > -1 {
						appendEntries := entries[conflictLoc : appendLen]
						conflictPos := appendStart + conflictLoc
						conflictPosInLog, conflictPosOk := rf.findLogIndex(conflictPos)
						if !conflictPosOk{
							log.Fatalf("index conflictPos %v is not exist in AppendEntries.\n", conflictPos)
						}

						rf.LogEntries = rf.LogEntries[:conflictPosInLog]
						rf.LogEntries = append(rf.LogEntries, appendEntries...)
						rf.maxLogIndex = appendEntries[len(appendEntries) - 1].LogIndex
						//fmt.Printf("in AE, conflict, append: %v, log: %v.\n", appendEntries,rf.LogEntries)
					} else {
						if lastLogIndex < args.Entries[appendLen - 1].LogIndex{
							appendEntries := entries[lastLogIndex-appendStart+1:]

							rf.LogEntries = rf.LogEntries[:lastIndexInLog + 1]
							rf.LogEntries = append(rf.LogEntries, appendEntries...)
							rf.maxLogIndex = appendEntries[len(appendEntries) - 1].LogIndex
							//fmt.Printf("in AE, append: %v, log: %v.\n", appendEntries,rf.LogEntries)
						}
						// append 的 log entries 如果相同 index 的 log 内容和该 rf 中的 log 相同，且 index 小于该 rf，
						// 则不做 append 操作。
					}
				} else {
					rf.LogEntries = append(rf.LogEntries, entries...)
					rf.maxLogIndex = entries[len(entries) - 1].LogIndex
				}
				//fmt.Printf("\nin append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, append done, content: %v.\n", rf.me, rf.VotedFor, args.PrevLogIndex, len(rf.LogEntries), rf.LogEntries)
			}
			reply.MatchIndex = args.Entries[appendLen - 1].LogIndex
			rf.persist()
		}
		rf.Mu.Unlock()
		if args.LeaderCommit <= lastLogIndex && rf.CommitIndex < args.LeaderCommit{
			go rf.Apply(args.LeaderCommit, false)
		}
		reply.Success = true
		reply.Term = rf.CurrentTerm
	}
	//fmt.Printf("append result before return: %v, me: %v\n", reply.Success, rf.me)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.Term= rf.CurrentTerm
	if args.Term < rf.CurrentTerm{
		return
	} else if args.LastIncludedIndex > rf.lastIncludeIndex{
		if rf.state != follower{
			rf.state = follower
		}
		rf.Persister.SaveSnapshot(args.Data)
		snapIndex := args.LastIncludedIndex
		rf.CommitIndex = snapIndex

		findIndex := -1
		for indexInLog, ele := range rf.LogEntries{
			if ele.LogIndex == args.LastIncludedIndex{
				findIndex = indexInLog
				break
			}
		}

		if findIndex > -1 && args.LastIncludedTerm == rf.LogEntries[findIndex].Term{
			rf.LogEntries = rf.LogEntries[findIndex + 1:]
		} else {
			rf.LogEntries = make([]LogEntry, 0)
		}

		if rf.CommitIndex < args.LastIncludedIndex{
			rf.CommitIndex = args.LastIncludedIndex
		}
		if rf.lastApplied < args.LastIncludedIndex{
			rf.lastApplied = args.LastIncludedIndex
		}
		if rf.maxLogIndex < args.LastIncludedIndex{
			rf.maxLogIndex = args.LastIncludedIndex
		}
		rf.lastIncludeIndex = args.LastIncludedIndex
		rf.lastIncludeTerm = args.LastIncludedTerm

		rf.persist()
		go rf.Apply(-1, true)
	}
}