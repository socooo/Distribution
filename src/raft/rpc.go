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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("servernum: %v, before start request vote,ask for vote: %v, args: %v, my state: %v, current commit: %v, term: %v.\n", rf.me, args.CandidateId, args, rf.state, rf.CommitIndex, rf.CurrentTerm)
	reason := 0
	reply.SendOk = true
	reply.Destination = rf.me
	reply.CandidateId = args.CandidateId
	if args.Term > rf.CurrentTerm && rf.state == leader{
		rf.state = follower
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
			rf.mu.Lock()
			rf.CurrentTerm = args.Term
			rf.mu.Unlock()
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 3
	} else if args.LastLogTerm < rf.LogEntries[rf.LogIndex].Term{
		if args.Term > rf.CurrentTerm {
			// 当一个服务器断开连接又重新连接后，可能会立即发起选举，此时他的term比其他服务器高，
			// 但是 CommitIndex 小于其他服务器，其他服务器需要同步他的 term ，否则 append 操作会一直无法完成。
			rf.mu.Lock()
			rf.CurrentTerm = args.Term
			rf.mu.Unlock()
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 2
	} else if args.LastLogTerm == rf.LogEntries[rf.LogIndex].Term && args.LastLogIndex < rf.LogIndex{
		if args.Term > rf.CurrentTerm {
			rf.mu.Lock()
			rf.CurrentTerm = args.Term
			rf.mu.Unlock()
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		reason = 4
	} else if rf.state != follower && args.CandidateId != rf.me{
		if args.Term > rf.CurrentTerm{
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.state = follower
			rf.CurrentTerm = args.Term
			rf.mu.Unlock()
			reply.Term = rf.CurrentTerm
		} else{
			reply.VoteGranted = false
			reply.Term = rf.CurrentTerm
		}
		reason = 5
	} else {
		reply.VoteGranted = true
		rf.mu.Lock()
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateId
		rf.mu.Unlock()
		reply.Term = rf.CurrentTerm
	}
	reply.Reason = reason
	rf.persist()
	rf.mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "RequestVote"))*time.Millisecond)
	rf.mu.Unlock()

	fmt.Printf("servernum: %v, before request vote return,ask for vote: %v, reply content, term:%v, vote granted:%v, my commit index: %v,args: %v, last log: %v, reason:%v.\n", rf.me, args.CandidateId, reply.Term, reply.VoteGranted, rf.CommitIndex, args, rf.LogEntries[rf.LogIndex], reason)

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendEntries"))*time.Millisecond)
	rf.mu.Unlock()
	fmt.Printf("In append entries, send from: %v, this serverNo: %v,current term: %v, args: %v, log: %v, log index: %v.\n", args.LeaderId, rf.me, rf.CurrentTerm, args, rf.LogEntries[rf.LogIndex], rf.LogIndex)
	if rf.LogIndex != rf.LogEntries[rf.LogIndex].LogIndex {
		log.Fatal("rf.LogIndex != index in entries.\n")
	}
	entries := args.Entries
	appendLen := len(entries)

	if args.Term < rf.CurrentTerm{
		//fmt.Printf("in append entries: this server: %v, leader no: %v args's term: %v, server's term: %v.\n", rf.me, rf.VotedFor, args.Term, rf.CurrentTerm)
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}else if rf.LogIndex >= args.PrevLogIndex && args.PrevLogTerm != rf.LogEntries[args.PrevLogIndex].Term{
		// 若此 server 为 leader，断线后，客户端继续发送了多个 LogEntries，
		// 重连后，如果在心跳时不检测最新的 log 的term，
		// 重新链接的老 leader 就会 Apply 这些在断开连接后客户端发送的LogEntries
		reply.ConflictIndex = rf.CommitIndex + 1
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}else if args.PrevLogIndex > rf.LogIndex{
		reply.ConflictIndex = rf.CommitIndex + 1
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	} else {
		if rf.state != follower && rf.CommitIndex <= args.LeaderCommit{
			// state 为 candidate 时接收到 AppendEntries RPC 说明落选，置为 follower；
			// state 为 leader 时收到 appendEntries RPC，可能是此服务器作为主服务器时断线，在新的主服务器选出后重连，
			// 此服务器的 CommitIndex 落后于发送 Append RPC 的服务器则表示此服务器时老的 Leader，重置为 follower 状态，
			// 此服务器的 CommitIndex 等于发送 Append RPC 的服务器，无法判断哪一个是老服务器，都置为 follower 重新选举。
			//fmt.Printf("this server: %v, state: %v, CommitIndex: %v, args: %v.\n", rf.me, rf.state, rf.CommitIndex, args)
			rf.mu.Lock()
			rf.state = follower
			rf.mu.Unlock()
			rf.persist()
		}
		rf.mu.Lock()
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderId
		rf.mu.Unlock()
		//fmt.Printf("\nin append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, receive entries content: %v.\n", rf.me, rf.VotedFor, args.PrevLogIndex, len(rf.LogEntries), entries)
		if entries != nil{
			//fmt.Printf("in AE, not hb, prev index: %v, term: %v.\n", args.PrevLogIndex, args.PrevLogTerm)
			//if entries[entriesLen - 1].LogIndex <= rf.LogIndex{
			//	if entries[entriesLen - 1].Command != rf.LogEntries[entries[entriesLen - 1].LogIndex].Command {
			//		// 一个主服务器在持久化了未commit的Log后崩溃，重启后恰好新主服务器的 logIndex 与该重启的服务器一样，但内容不一样
			//		// 鉴于这种情况需要在心跳时检查主服务器最新索引的信息是否与该服务器同样索引的log内容一致。
			//		reply.ConflictIndex = rf.CommitIndex + 1
			//		reply.Success = false
			//		reply.Term = rf.CurrentTerm
			//		return
			//	}
			//
			//}
			appendStart := args.PrevLogIndex + 1
			if args.PrevLogIndex == 0{
				if len(rf.LogEntries) > appendStart{
					rf.LogEntries = rf.LogEntries[0:1]
				}
				rf.LogEntries = append(rf.LogEntries, args.Entries...)
				rf.LogIndex = rf.LogEntries[len(rf.LogEntries)-1].LogIndex
				//fmt.Printf("in append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, append init done, content: %v.\n", rf.me, rf.VotedFor, args.PrevLogIndex, len(rf.LogEntries), rf.LogEntries[len(rf.LogEntries)-1])
			}else if args.PrevLogIndex > rf.CommitIndex{
				// 若此 server 断线重连后， CommitIndex 可能落后于主服务器很多条 LogEntries，需要重新发送这些 LogEntries。
				reply.ConflictIndex = rf.CommitIndex + 1
				reply.Success = false
				reply.Term = rf.CurrentTerm
				//fmt.Printf("in AE, conflict, this: %v, conflict: %v, leader: %v, args content: %v.\n", rf.me, reply.ConflictIndex, args.LeaderId, args)
				return
			} else {
				//fmt.Printf("in AE, start append, this: %v, leader: %v, args content: %v.\n", rf.me, args.LeaderId, args)
				if rf.LogIndex >= appendStart{
					conflictLoc := -1
					for i := 0; i < appendLen; i++ {
						compareIndex := entries[i].LogIndex
						if compareIndex > rf.LogIndex {
							break
						}
						fmt.Printf("in AE, compare index: %v.\n", compareIndex)
						if rf.LogEntries[compareIndex].Term != entries[i].Term{
							conflictLoc = i
							break
						}
					}
					if conflictLoc > -1 {
						rf.LogEntries = rf.LogEntries[:appendStart]
						rf.LogEntries = append(rf.LogEntries, entries...)
						rf.LogIndex = rf.LogEntries[len(rf.LogEntries)-1].LogIndex
					} else {
						if rf.LogIndex < args.Entries[appendLen - 1].LogIndex{
							rf.LogEntries = rf.LogEntries[:appendStart]
							rf.LogEntries = append(rf.LogEntries, entries...)
							rf.LogIndex = rf.LogEntries[len(rf.LogEntries)-1].LogIndex
						}
						// append 的 log entries 如果相同 index 的 log 内容和该 rf 中的 log 相同，且 index 小于该 rf，
						// 则不做 append 操作。
					}
				} else {
					rf.LogEntries = append(rf.LogEntries, entries...)
					rf.LogIndex = rf.LogEntries[len(rf.LogEntries)-1].LogIndex
				}
				//fmt.Printf("\nin append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, append done, content: %v.\n", rf.me, rf.VotedFor, args.PrevLogIndex, len(rf.LogEntries), rf.LogEntries)
			}
			reply.MatchIndex = args.Entries[appendLen - 1].LogIndex
			rf.persist()
		}
		if rf.CommitIndex < args.LeaderCommit{
			go rf.Apply(args.LeaderCommit)
		}
		reply.Success = true
		reply.Term = rf.CurrentTerm
		return
	}
	//fmt.Printf("append result before return: %v\n", reply.Success)
}
