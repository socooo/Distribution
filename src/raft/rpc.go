package raft

import (
	"time"
)

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("servernum: %v, before start request vote,ask for vote: %v, term:%v, my state: %v.\n", rf.me, args.CandidateId, args.Term, rf.state)
	rf.mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "RequestVote"))*time.Millisecond)
	rf.mu.Unlock()
	reply.SendOk = true
	reply.Destination = rf.me
	reply.CandidateId = args.CandidateId
	if args.Term > rf.currentTerm && rf.state == leader{
		rf.state = follower
	}
	if args.CandidateId == rf.me{
		reply.VoteGranted = true
		rf.votedFor = rf.me
		reply.Term = rf.currentTerm
	} else if rf.currentTerm >= args.Term ||
		args.LastLogTerm < rf.currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}else if args.LastLogIndex < rf.commitIndex{
		if args.Term > rf.currentTerm {
			// 当一个服务器断开连接又重新连接后，可能会立即发起选举，此时他的term比其他服务器高，
			// 但是 commitIndex 小于其他服务器，其他服务器需要同步他的 term ，否则 append 操作会一直无法完成。
			rf.mu.Lock()
			rf.currentTerm = args.Term
			rf.mu.Unlock()
		}
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if rf.state != follower && args.CandidateId != rf.me{
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
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
	}
	//fmt.Printf("servernum: %v, before request vote return,ask for vote: %v, reply content, term:%v, vote granted:%v, my state: %v， my term: %v.\n", rf.me, args.CandidateId, reply.Term, reply.VoteGranted, rf.state, rf.currentTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(genVoteTimeOut(rf.me, "AppendEntries"))*time.Millisecond)
	rf.mu.Unlock()
	//fmt.Printf("In append entries, send from: %v, this serverNo: %v,current term: %v, args: %v.\n", args.LeaderId, rf.me, rf.currentTerm, args)
	if args.Term < rf.currentTerm{
		//fmt.Printf("in append entries: this server: %v, leader no: %v args's term: %v, server's term: %v.\n", rf.me, rf.votedFor, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}else if rf.logIndex >= args.PrevLogIndex && args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term{
		// 若此 server 为 leader，断线后，客户端继续发送了多个 LogEntries，
		// 重连后，如果在心跳时不检测最新的 log 的term，
		// 重新链接的老 leader 就会 Apply 这些在断开连接后客户端发送的logEntries
		reply.ConflictIndex = rf.commitIndex + 1
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		if rf.state != follower && rf.commitIndex <= args.LeaderCommit{
			// state 为 candidate 时接收到 AppendEntries RPC 说明落选，置为 follower；
			// state 为 leader 时收到 appendEntries RPC，可能是此服务器作为主服务器时断线，在新的主服务器选出后重连，
			// 此服务器的 commitIndex 落后于发送 Append RPC 的服务器则表示此服务器时老的 Leader，重置为 follower 状态，
			// 此服务器的 commitIndex 等于发送 Append RPC 的服务器，无法判断哪一个是老服务器，都置为 follower 重新选举。
			//fmt.Printf("this server: %v, state: %v, commitIndex: %v, args: %v.\n", rf.me, rf.state, rf.commitIndex, args)
			rf.mu.Lock()
			rf.state = follower
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.mu.Unlock()
		entries := args.Entries
		//fmt.Printf("\nin append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, receive entries content: %v.\n", rf.me, rf.votedFor, args.PrevLogIndex, len(rf.logEntries), entries)
		if entries != nil{
			//fmt.Printf("in AE, not hb, prev index: %v, term: %v.\n", args.PrevLogIndex, args.PrevLogTerm)
			appendStart := args.PrevLogIndex + 1
			if args.PrevLogIndex == 0{
				if len(rf.logEntries) > appendStart{
					rf.logEntries = rf.logEntries[0:1]
				}
				rf.logEntries = append(rf.logEntries, args.Entries...)
				rf.logIndex = rf.logIndex + len(args.Entries)
				//fmt.Printf("in append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, append init done, content: %v.\n", rf.me, rf.votedFor, args.PrevLogIndex, len(rf.logEntries), rf.logEntries[len(rf.logEntries)-1])

			} else if args.PrevLogIndex > rf.commitIndex{
				// 若此 server 断线重连后， commitIndex 可能落后于主服务器很多条 logEntries，需要重新发送这些 logEntries。
				reply.ConflictIndex = rf.commitIndex + 1
				reply.Success = false
				reply.Term = rf.currentTerm
				//fmt.Printf("in AE, conflict, this: %v, conflict: %v, leader: %v, args content: %v.\n", rf.me, reply.ConflictIndex, args.LeaderId, args)
				return
			} else {
				//fmt.Printf("in AE, start append, this: %v, leader: %v, args content: %v.\n", rf.me, args.LeaderId, args)
				if rf.logIndex >= appendStart{
					rf.logEntries = rf.logEntries[:appendStart]
				}
				rf.logEntries = append(rf.logEntries, args.Entries...)
				rf.logIndex = rf.logIndex + len(args.Entries)
				//fmt.Printf("\nin append entries: this server: %v, leader no: %v PrevIndex: %v, log len: %v, append done, content: %v.\n", rf.me, rf.votedFor, args.PrevLogIndex, len(rf.logEntries), rf.logEntries)
			}
		}
		if rf.commitIndex < args.LeaderCommit{
			go rf.Apply(args.LeaderCommit)
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	//fmt.Printf("append result before return: %v\n", reply.Success)
}

