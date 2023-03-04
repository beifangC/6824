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
	"bytes"
	"math/rand"
	"sort"
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
	CommandValid bool // true为log，false为snapshot

	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// 向application层安装快照
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
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
	lastIncludedTerm  int     // snapthost最后1个logEntry的term

	// 所有服务器，易失状态
	commitIndex int // 最大已提交索引
	lastApplied int // 应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	同步起点索引
	matchIndex []int // 同步进度 0

	state             RaftState
	leaderId          int
	lastActiveTime    time.Time // 超时机制（leader心跳、投票、请求投票）
	lastBroadcastTime time.Time

	applyCh chan ApplyMsg //提交chan
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	//
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
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
	//
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := rf.lastTerm()
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now()
		}
	}

	rf.persist()
}

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
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool //true -> matching log
	//方便做日志同步
	ConflictIndex int //第一个冲突日志
	ConflictTerm  int
}

//AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	//拒绝响应
	if rf.currentTerm > args.Term {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)

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

	// 如果prevLogIndex在快照内，且不是快照最后一个log，失配
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1 //从1号开始重新匹配
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogTerm != rf.lastIncludedTerm { //任期不同
			reply.ConflictIndex = 1
			return
		}
	} else { // prevLogIndex在快照之后，那么进一步判定
		if args.PrevLogIndex > rf.lastIndex() { // prevLogIndex位置没有日志的case
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		// prevLogIndex位置有日志，
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
	}

	// 保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() { //直接追加日志
			rf.log = append(rf.log, logEntry)
		} else { // 有重叠日志
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除重复
				rf.log = append(rf.log, logEntry) // 追加
			}
		}
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}
	rf.persist()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index = rf.lastIndex()
	term = rf.currentTerm
	//rf.persist()
	DPrintf("rf[%v] new commond, Index[%d] Term[%d]", rf.me, index, term)
	return index, term, isLeader
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
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader {
				return
			}

			// 100ms
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			// 向所有follower发送心跳
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				// 如果nextIndex在leader的snapshot内，那么直接同步snapshot
				if rf.nextIndex[peerId] > rf.lastIncludedIndex {
					rf.appendEntries(peerId)
				}
			}
		}()
	}
}

func (rf *Raft) appendEntries(peerId int) {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Entries = make([]Entry, 0)
	args.PrevLogIndex = rf.nextIndex[peerId] - 1

	// 如果prevLogIndex是leader快照的最后1条log, 那么取快照的最后1个term
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else {
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)

	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 状态改变
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.convertToFollower(reply.Term)
				rf.leaderId = -1
				rf.persist()
				return
			}

			if reply.Success { // 同步日志成功
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				rf.updateCommitIndex() // 更新commitIndex
			} else {
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 { // leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex // 用follower首次出现term的index作为同步开始
					}
				} else {
					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
					// 这时候我们将返回的conflictIndex设置为nextIndex即可
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
				DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
			}
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	//保证commitIndex覆盖一半节点，找中位数
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	// 如果index属于snapshot范围，那么不要检查term了
	// 否则还是检查log的term是否满足条件
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastIncludedIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}

}

//提交日志
func (rf *Raft) applyLog() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg
				noMore = false
			}
		}()
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

	rf.state = Follower
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.broadcastHeartBeat()
	go rf.applyLog()
	return rf
}
