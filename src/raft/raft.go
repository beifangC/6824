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

import (
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//thre e states of server
type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
	Index   int // identify its position in the log
	Term    int //
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	log         []Entry
	timeout     time.Time //超时的时间
	state       RaftState

	commitIndex int //highest index of committed log
	lastApplied int

	nextIndex  []int //Assumed matching items
	matchIndex []int //realy matching items

	//recHeart bool

	applyCh chan ApplyMsg //test 2b
}

// return currentTerm and whether this server
// believes it is the leader. only for test of 2A
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	//DPrintf("rf[%v] is leader ? %v", rf.me, rf.state == Leader)
	return rf.currentTerm, rf.state == Leader
}

// return last log
func (rf *Raft) getLastLog() Entry {
	l := len(rf.log)
	if l == 0 {
		return Entry{0, 0, nil}
	}
	return rf.log[l-1]
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool //ture -> received vote
}

//RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// receive rpc

	// DPrintf("current term is %v args term is %v", rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//当前任期更新，拒绝投票
	if rf.currentTerm > args.Term {
		return
	}

	rf.resetTimeout()
	//if term > currentTerm,set currentTerm = term,convert to follower
	if args.Term > rf.currentTerm {
		// DPrintf("rf[%v] convert to follower", rf.me)

		rf.currentTerm = args.Term
		rf.convertFollower()

	}

	llog := rf.getLastLog()
	//DPrintf("args:%v,rf[%v] term:%v", args.LastLogTerm, rf.me, rf.currentTerm)
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && (args.Term > llog.Term || (llog.Term == args.LastLogTerm && args.LastLogIndex > llog.Index)) {
		// DPrintf("%v vote for candidate %v", rf.me, args.CandidateId)
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// leader send to others
type AppendEntriesArgs struct {
	Term         int //leader's tram
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry //empty for heartbeat
	LedaerCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool //true -> matching log
}

//AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	reply.Term = rf.currentTerm
	reply.Success = false
	//拒绝响应
	if rf.currentTerm > args.Term {
		return
	}
	rf.resetTimeout()

	//if recetive term >currentTerm
	if rf.currentTerm < args.Term {
		//DPrintf("rf %v convert to follower", rf.me)
		rf.currentTerm = args.Term
		rf.convertFollower()
	}

	// heartbeats,doesn't need care about log recliption
	//if len(args.Entries) == 0 {
	// reply.Success = true
	//	return
	//}

	//本地日志缺少
	if len(rf.log) < args.PrevLogIndex {
		DPrintf("log missing len(log): %v prevlogindex:%v", len(rf.log), args.PrevLogIndex)
		return
	}
	//日志term不匹配
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("term not fit")
		return
	}
	// DPrintf("args.Prevlogindex:%v len of log:%v", args.PrevLogIndex, len(rf.log))
	// DPrintf("args.Entries:%v", args.Entries)
	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + i + 1

		if logIndex >= len(rf.log)-1 {
			DPrintf("rf[%v]apply log", rf.me)
			rf.log = append(rf.log, entry)
		} else {
			if rf.log[logIndex].Term != entry.Term {
				rf.log = rf.log[:logIndex] //删除失配内容
				rf.log = append(rf.log, entry)
			}
		}
	}
	//DPrintf(" raft %v log replication", rf.me)

	//更新commitdex
	if args.LedaerCommit > rf.commitIndex {
		//DPrintf("leadercommit:%v", args.LedaerCommit)
		rf.commitIndex = args.LedaerCommit
		if rf.commitIndex > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		}
		//DPrintf("update commitindex %v", rf.commitIndex)
	}

	go rf.applyLog()

	reply.Success = true
}

// send AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).

	index := -1
	term := -1
	if rf.state != Leader {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.log)

	term = rf.currentTerm
	rf.log = append(rf.log, Entry{Term: term, Command: command, Index: index})

	DPrintf("new command %v at term %v", command, term)
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//成为leader之后不需要检测超时信息
		if rf.state == Leader {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		// in paper, broadcast time may range 0.5ms to 20ms,depending on storage technology
		// heartbeats interval is 100ms
		var dura time.Duration

		if time.Now().After(rf.timeout) {
			rf.leaderElection()
			continue
		}
		//修改时间sleep时间使得最后超时马上响应，而不是heartbeat的倍数
		if rf.timeout.Sub(time.Now()) < 150*time.Millisecond {
			dura = rf.timeout.Sub(time.Now())
		} else {
			dura = 150 * time.Millisecond
		}

		time.Sleep(dura)
	}
}

//start leader election phase
func (rf *Raft) leaderElection() {
	DPrintf("rf[%v] start leader election", rf.me)
	if rf.killed() == true {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.resetTimeout()
	rf.broadcastRequsetVote()
}

//reset the time
func (rf *Raft) resetTimeout() {

	rand.Seed(time.Now().UnixMicro())
	//随机200-350ms
	dura := rand.Int63n(200) + 300
	rf.timeout = time.Now().Add(time.Duration(dura) * time.Millisecond)
}

//广播RequsetVote，并处理结果
func (rf *Raft) broadcastRequsetVote() {
	if rf.killed() || rf.state != Candidate {
		DPrintf("%v candidate 终止", rf.me)
		return
	}

	count := 1 //have got a vote from itself
	llog := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: llog.Index,
		LastLogTerm:  llog.Term,
	}
	var once sync.Once
	for i := range rf.peers {
		//跳过自己
		if i == rf.me {
			continue
		}
		if rf.state != Candidate {
			//终止自己的选举过程
			return
		}

		go func(i int) {
			reply := RequestVoteReply{}
			f := rf.sendRequestVote(i, &args, &reply)
			//处理返回结果
			//任期不是最新的，变为folloer
			if f && reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.resetTimeout()
			}
			//的到投票
			if f && reply.VoteGranted {
				count++
				//receive majority
				if count > len(rf.peers)/2 {
					//make sure only run once
					once.Do(func() {
						rf.convertLeader()
						rf.broadcastHeartBeat()
					})
					return
				}
			}
		}(i)
	}
	//DPrintf("rf %v  get %v vote of %v", rf.me, count, len(rf.peers))
}

func (rf *Raft) convertLeader() {
	DPrintf("rf[%v] convert to leader", rf.me)
	rf.state = Leader
	for i := range rf.peers {
		//initialized to leader last log index+1
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) convertFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.voteFor = -1
}

//发起心跳广播
func (rf *Raft) broadcastHeartBeat() {
	DPrintf("rf[%v] start heartbeats", rf.me)
	for !rf.killed() && rf.state == Leader {
		for i := range rf.peers {
			//跳过自己
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := AppendEntriesReply{}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LedaerCommit: rf.commitIndex,
					Entries:      make([]Entry, 0),
				}
				//DPrintf("nextIndex[%v]:%v", i, rf.nextIndex[i])
				if args.PrevLogIndex >= 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}
				if rf.nextIndex[i] >= 0 {
					args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]:]...)
				}

				DPrintf("sendappend to rf[%v] Entries :%v", i, len(args.Entries))
				//send RPC
				f := rf.sendAppendEntries(i, &args, &reply)
				//链接失败
				if !f {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.convertFollower()
					return
				}
				//发送的是心跳，接下来的返回消息可以不用处理
				if len(args.Entries) == 0 {
					return
				}

				// sync success
				if reply.Success {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					//更新commiindex
					arr := make([]int, len(rf.matchIndex))
					copy(arr, rf.matchIndex)
					//添加leader节点的
					arr[rf.me] = len(rf.log) - 1
					sort.Ints(arr)
					newCIndex := arr[len(rf.peers)/2]
					if rf.state == Leader && rf.log[newCIndex].Term == rf.currentTerm && newCIndex > rf.commitIndex {
						rf.commitIndex = newCIndex
						DPrintf("leader commit index update:%v", newCIndex)
						go rf.applyLog()
					}

				} else { //sync fail
					rf.nextIndex[i] = args.PrevLogIndex
				}
			}(i)
		}
		time.Sleep(100 * time.Millisecond)
	}

}

//追加日志
func (rf *Raft) applyLog() {
	if len(rf.log) == 0 {
		//DPrintf("rf[%v] len==0 return", rf.me)
		return
	}

	//DPrintf("rf[%v]commitindex:%v,lastApplied:%v", rf.me, rf.commitIndex, rf.lastApplied)
	//DPrintf("rf[%v]commit log", rf.me)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		//DPrintf("rf.nextIndex[]:%v", rf.nextIndex)

		rf.applyCh <- msg
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1 // vote for no one
	rf.state = Follower
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0
	rf.log = make([]Entry, 0)
	// rf.log = append(rf.log, Entry{
	// 	Index:   0,
	// 	Term:    0,
	// 	Command: nil,
	// })
	rf.applyCh = applyCh //这个chan从参数传过来的
	rf.resetTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.peintlog()
	return rf
}

func (rf *Raft) peintlog() {
	for !rf.killed() {
		DPrintf("rf[%v] commitindex:%v log: %v", rf.me, rf.commitIndex, rf.log)
		time.Sleep(1 * time.Second)
	}
}
