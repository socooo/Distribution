package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
)


type Clerk struct {
	id int64
	servers []*labrpc.ClientEnd
	lastLeader int
	requestIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.requestIndex = -1
	ck.id = nrand()
	return ck
}

const retryInterval = 80*time.Microsecond
//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) sendRequest(){

}
func (ck *Clerk) Get(key string) string {
	ck.requestIndex = ck.requestIndex + 1
	args := GetArgs{RequestIndex: ck.requestIndex, ClientId: ck.id, Key:key}
	reply := GetReply{}
	serverQuan := len(ck.servers)
	lastLeader := ck.lastLeader
	for{
		ok := ck.servers[lastLeader].Call("RaftKV.Get", &args, &reply)
		if ok{
			// fmt.Printf("in client Get,reply: %v.\n", reply)
			if reply.Err == OK{
				ck.lastLeader = lastLeader
				//fmt.Printf("in client get, ok, return, args: %v, reply: %v.\n", args, reply)
				return reply.Value
			} else if reply.Err == WrongLeader || reply.Err == TimeOut {
				lastLeader = (lastLeader + 1) % serverQuan
				//fmt.Printf("in client get, change leader no: %v, err: %v.\n", lastLeader, reply.Err)
			} else if reply.Err == ErrNoKey {
				//println("in client get, no such key, return.\n")
				return ""
			}
		} else {
			lastLeader = (lastLeader + 1) % serverQuan
		}
		time.Sleep(retryInterval)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestIndex = ck.requestIndex + 1
	args := PutAppendArgs{RequestIndex: ck.requestIndex, ClientId: ck.id, Key:key,Value:value,Op:op}
	reply := PutAppendReply{}
	serverQuan := len(ck.servers)
	lastLeader := ck.lastLeader

	for{
		//fmt.Printf("in client send putappend, to: %v, key: %v, value: %v.\n", lastLeader, key, value)
		ok := ck.servers[lastLeader].Call("RaftKV.PutAppend", &args, &reply)
		if ok{
			if reply.Err == OK{
				ck.lastLeader = lastLeader
				//fmt.Printf("in client putAppend, ok, return, args: %v, reply: %v.\n", args, reply)
				return
			} else if reply.Err == WrongLeader || reply.Err == TimeOut {
				lastLeader = (lastLeader + 1) % serverQuan
				//fmt.Printf("in client putAppend, change leader no: %v, err: %v.\n", lastLeader, reply.Err)
			}
		} else {
			lastLeader = (lastLeader + 1) % serverQuan
		}
		time.Sleep(retryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
