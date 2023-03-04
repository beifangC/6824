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

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

	currentTerm       int
	votedFor          int
	log               []Entry // 日志
	lastIncludedIndex int     // snapshot最后1个logEntry的index，没有snapshot则为0
	lastIncludedTerm  int     // snapthost最后1个logEntry的term，没有snaphost则无意义

	// 所有服务器，易失状态
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0）

	state             RaftState
	leaderId          int
	lastActiveTime    time.Time // 超时机制（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time

	applyCh chan ApplyMsg // 应用层的提交队列
}

// return currentTerm and whether this server
// believes it is the leader. only for test of 2A
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// persistent : currentTerm voteFor log
	// Example:

	// DPrintf("rf[%v] persist { cur:%v votefor:%v log:%v}", rf.me, rf.currentTerm, rf.voteFor, rf.log)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

	// DPrintf("rf[%v] readpersist {cur:%v votefor:%v log%v}", rf.me, rf.currentTerm, rf.voteFor, rf.log)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.leaderId = -1
	}
	// 每个任期，只能投票给1人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastActiveTime = time.Now()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	//reply.ConflictIndex = -1
	//reply.ConflictTerm = -1

	//拒绝响应
	if rf.currentTerm > args.Term {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		//rf.persist()
	}
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()

	/* 22年老方法
	//本地日志缺少
	if len(rf.log)-1 < args.PrevLogIndex {
		DPrintf("rf[%v] log missing len(log): %v prevlogindex:%v", rf.me, len(rf.log), args.PrevLogIndex)
		reply.Success = false
		return
	}
	//日志term不匹配
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("term not fit")
		reply.Success = false
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
	rf.persist()
	//DPrintf(" raft %v log replication", rf.me)

	reply.Success = true

	//更新commitdex
	if args.LedaerCommit > rf.commitIndex {
		//DPrintf("leadercommit:%v", args.LedaerCommit)
		rf.commitIndex = args.LedaerCommit
		if rf.commitIndex > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		}
		//DPrintf("update commitindex %v", rf.commitIndex)
	}

	rf.applyLog()
	*/

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
	DPrintf("rf[%v] was killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时随机化
			elapses := now.Sub(rf.lastActiveTime)

			if rf.state == Follower {
				if elapses >= timeout {
					rf.state = Candidate
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}

			// 请求vote
			if rf.state == Candidate && elapses >= timeout {
				rf.lastActiveTime = now // 重置下次选举时间
				rf.currentTerm += 1     // 发起新任期
				rf.votedFor = rf.me     // 该任期投了自己
				//rf.persist()

				rf.broadcastRequestVote()
			}
		}()
	}
}

//广播RequsetVote，并处理结果
func (rf *Raft) broadcastRequestVote() {
	// 请求投票req
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
	}
	args.LastLogTerm = rf.lastTerm()

	rf.mu.Unlock()
	DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
		args.LastLogIndex, args.LastLogTerm)
	// 并发RPC请求vote
	voteCount := 1   // 收到投票个数（先给自己投1票）
	finishCount := 1 // 收到应答个数
	voteResultChan := make(chan *RequestVoteReply, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		go func(id int) {
			if id == rf.me {
				return
			}
			resp := RequestVoteReply{}
			if ok := rf.sendRequestVote(id, &args, &resp); ok {
				voteResultChan <- &resp
			} else {
				voteResultChan <- nil
			}
		}(i)
	}

	maxTerm := 0
	for {
		select {
		case voteResult := <-voteResultChan:
			finishCount += 1
			if voteResult != nil {
				if voteResult.VoteGranted {
					voteCount += 1
				}
				if voteResult.Term > maxTerm {
					maxTerm = voteResult.Term
				}
			}
			// 得到大多数vote后，立即离开,得到所有恢复后也离开
			if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
				goto VOTE_END
			}
		}
	}
VOTE_END:
	rf.mu.Lock()

	// 如果角色改变了，则忽略本轮投票结果
	if rf.state != Candidate {
		return
	}
	// 发现了更高的任期，切回follower
	if maxTerm > rf.currentTerm {
		rf.convertToFollower(maxTerm)
		rf.leaderId = -1

		//rf.persist()
		return
	}
	// 赢得大多数选票，则成为leader
	if voteCount > len(rf.peers)/2 {
		rf.convertToLeader()
		return
	}
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.leaderId = rf.me

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
	rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
}

func (rf *Raft) convertToCanditate() {

}

// 最后的index
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// 最后的term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastIncludedTerm // for snapshot
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

// 日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - rf.lastIncludedIndex - 1
}

//发起心跳广播
func (rf *Raft) broadcastHeartBeat() {

}

//提交日志
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
	rf.votedFor = -1 // vote for no one
	rf.state = Follower
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0
	rf.log = make([]Entry, 0)

	rf.log = append(rf.log, Entry{
		Index:   0,
		Term:    0,
		Command: nil,
	})

	rf.applyCh = applyCh //这个chan从参数传过来的

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
