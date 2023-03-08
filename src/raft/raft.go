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

	SnapshotValid bool
	Snapshot      []byte //存储kv数据，请求编号，应用层来实现
	//last Index Term单独存储
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

	currentTerm int
	votedFor    int
	log         []Entry // 日志
	//快照保存index和term，
	lastIncludedIndex int
	lastIncludedTerm  int

	commitIndex int // 最大已提交索引

	lastApplied int // 应用到状态机的索引

	nextIndex  []int //	下一个应该发送
	matchIndex []int // 同步位置

	state             RaftState
	leaderId          int
	lastActiveTime    time.Time // 超时时间起点
	lastBroadcastTime time.Time // 广播时间起点

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
	rf.persister.SaveRaftState(rf.persistDate())
}

// 返回序列化字节数据
func (rf *Raft) persistDate() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	//持久化快照的index和term
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
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

/*
* 进行一次快照，即日志的压缩，不再维护早期的日志信息，snapshot是应用层传来
 */
func (rf *Raft) Snapshot(lastIncludedIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 压缩日志长度
	loglen := lastIncludedIndex - rf.lastIncludedIndex

	rf.lastIncludedTerm = rf.log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.lastIncludedIndex = lastIncludedIndex

	// 在log中删除snapshot部分
	taillog := make([]Entry, len(rf.log)-loglen)
	copy(taillog, rf.log[loglen:])
	rf.log = taillog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.persistDate(), snapshot)

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
	DPrintf("rf[%v] receive requestvote from rf[%v]", rf.me, args.CandidateId)

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

	/* 22年
	//本地日志缺少
	if len(rf.log)-1 < args.PrevLogIndex {
		DPrintf("rf[%v] log missing len(log): %v prevlogindex:%v", rf.me, len(rf.log), args.PrevLogIndex)
		reply.Success = false
		return
	}
	//日志term不匹配
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

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

	//查找日志是否匹配
	// 如果prevLogIndex在快照内，且不是快照最后一个log，失配
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1 //从1号开始重新匹配
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogTerm != rf.lastIncludedTerm { //任期不同
			reply.ConflictIndex = 1
			return
		}
	} else { // prevLogIndex在快照之后 args.PrevLogIndex > rf.lastIncludedIndex
		if args.PrevLogIndex > rf.lastIndex() { //本地日志缺少
			reply.ConflictIndex = rf.lastIndex() + 1 //补充缺少日志
			return
		}
		/*
		*如果follower有prevLogIndex日志，但是term不匹，返回
		*conflictTerm = log[prevLogIndex].Term,并且寻找第一个有
		*相同term的index，并返回ConflictIndex = index
		 */
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
		rf.commitIndex = args.LeaderCommit   //同步leader 提交进度
		if rf.lastIndex() < rf.commitIndex { //如果本身len（log）不够
			rf.commitIndex = rf.lastIndex()
		}
	}

	go rf.applyLogOnce()

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
	rf.persist()
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
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond //超市时间

			//DPrintf("rf[%v] timeout:%v", rf.me, timeout)
			interval := now.Sub(rf.lastActiveTime) //现经过时间
			if rf.state == Follower {
				if interval >= timeout {
					rf.state = Candidate
					DPrintf("rf[%v] start election", rf.me)
				}
			}

			// 请求vote
			if rf.state == Candidate && interval >= timeout {
				rf.lastActiveTime = now
				rf.currentTerm += 1
				rf.votedFor = rf.me
				//rf.persist()
				rf.broadcastRequestVote()
			}
		}()
	}
}

//广播RequsetVote
func (rf *Raft) broadcastRequestVote() {
	DPrintf("rf[%v] broadcastRequestVote", rf.me)
	// 请求投票req
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
	}
	args.LastLogTerm = rf.lastTerm()

	rf.mu.Unlock()

	voteCount := 1   // 收到投票
	finishCount := 1 // 收到回答

	/*
	 *发送requestvote，并行进行能够节省时间，我们利用chan将结果传出，串行
	 *处理，否则容易出现rf重复的进入Follower状态，然后重复唤醒headerbeat
	 */
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
			/*
			*我们利用select和for循环，读区传出的数据，为了防止卡死在此处，当我们
			*收到所有节点的恢复时，即使没有成功返回或者报告错误
			 */
			if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
				goto VOTE_END
			}
		}
	}
VOTE_END:
	rf.mu.Lock()

	/*
	*在requestvote期间有其他点成为了leader并且发送heartbeat，
	*本节身份降为follower，应该停止election过程
	 */
	if rf.state != Candidate {
		return
	}
	// 对方的任期更高
	if maxTerm > rf.currentTerm {
		rf.convertToFollower(maxTerm)
		rf.leaderId = -1
		return
	}
	// 得到多数票
	if voteCount > len(rf.peers)/2 {
		rf.convertToLeader()
		return
	}
}

//发起心跳广播
func (rf *Raft) broadcastHeartBeat() {
	for !rf.killed() {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才能进行heartbeat
			if rf.state != Leader {
				return
			}

			// 广播周期100ms
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}

			rf.lastBroadcastTime = time.Now()
			DPrintf("rf[%v] broadcastHeartBeat", rf.me)
			// 向所有follower发送心跳
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				// nextIndex在snapshot内，同步snapshot
				if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
					rf.doInstallSnapshot(peerId)
				} else { // 同步日志
					rf.appendEntries(peerId)
				}
			}

			go rf.applyLogOnce()
			time.Sleep(10 * time.Millisecond)
		}()
	}
}

// 发送日志追加
func (rf *Raft) appendEntries(peerId int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]Entry, 0), //为空是心跳
	}
	args.PrevLogIndex = rf.nextIndex[peerId] - 1

	// prevLogIndex是leader快照的最后1条，则term也相同
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else { //从log[]取得最后term
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	//封装日志内容
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)

	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 状态改变，及时退出
			if rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				rf.leaderId = -1
				rf.persist()
				return
			}

			if reply.Success { // 同步日志成功
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				rf.updateCommitIndex() // 更新commitIndex
			} else { //日志不匹配，需要重新找到匹配点
				/*
				* 当日志不匹配时，只能是nextIndex后退，因为数据只能leader->follower，我们可以每次
				* nextIndex--，但是这种效率不高，因为要多个RPC后才能配合上，
				*
				* 如果收到冲突返回，leader应该先查找自己log有相同conflictTerm，如果找到了，
				* 设置nextIndex为这个term中log.Index+1
				* 如果没有，则是指nextIndex为conflictIndex
				 */
				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 {
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex
					}
				} else {
					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
			}
		}
	}()
}

//leader中值找commitIndex
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
	// 如果index属于snapshot范围，那么不要检查term了，是已经被提交的内容
	// 只有本term的才能被提交,figure8,commitIndex不修改，applyLog就无法提交
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastIncludedIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
}

//提交日志
func (rf *Raft) applyLogOnce() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	for rf.commitIndex > rf.lastApplied {
		//每次只提交一个
		rf.lastApplied += 1
		appliedIndex := rf.index2LogPos(rf.lastApplied)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[appliedIndex].Command,
			CommandIndex: rf.lastApplied,
			CommandTerm:  rf.log[appliedIndex].Term,
		}
		rf.applyCh <- applyMsg
	}
}

/*
//提交日志
func (rf *Raft) applyLog() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.commitIndex > rf.lastApplied {
				//每次只提交一个
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg
			}
		}()
	}
}
*/

// ###############Snapshot##############
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// 安装快照RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
	}

	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()

	// leader快照不如本地长，那么忽略这个快照
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else { // leader快照比本地快照长
		if args.LastIncludedIndex < rf.lastIndex() { // 快照外还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]Entry, 0) // term冲突，扔掉快照外的所有日志
			} else { // term没冲突，保留后续日志
				leftLog := make([]Entry, rf.lastIndex()-args.LastIncludedIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftLog
			}
		} else { // 快照外没有日志，覆盖本地
			rf.log = make([]Entry, 0)
		}
	}
	// 更新快照位置
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 将传来的snapshot Data保存到本地
	rf.persister.SaveStateAndSnapshot(rf.persistDate(), args.Data)

	rf.installSnapshotToApplication()

}

func (rf *Raft) doInstallSnapshot(peerId int) {
	DPrintf("rf[%v] InstallSnapshot starts,perrId[%v]\n", rf.me, peerId)

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Done:              true,
	}
	args.Data = rf.persister.ReadSnapshot() //保存持久化信息

	reply := InstallSnapshotReply{}
	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 及时停止
			if rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				rf.leaderId = -1
				rf.persist()
				return
			}

			//更新log匹配值
			rf.nextIndex[peerId] = rf.lastIndex() + 1      // 重新从末尾同步log
			rf.matchIndex[peerId] = args.LastIncludedIndex // 已同步到的位置
			rf.updateCommitIndex()

		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installSnapshotToApplication() {
	//var applyMsg *ApplyMsg

	// 同步给application层的快照
	applyMsg := &ApplyMsg{
		SnapshotValid:     true,
		CommandValid:      false,
		Snapshot:          rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}

	rf.lastApplied = rf.lastIncludedIndex

	rf.applyCh <- *applyMsg
	return
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

	rand.Seed(time.Now().Unix())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// make sure install snapshot immediately after restart
	rf.installSnapshotToApplication()

	DPrintf("rf[%v] start wit activetime:%v", rf.me, rf.lastActiveTime)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// ##############工具方法##################

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
}

func (rf *Raft) convertToLeader() {
	DPrintf("rf[%v] convert to leader", rf.me)
	rf.state = Leader
	rf.leaderId = rf.me
	// 初始化日志匹配数据
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	//开启广播
	rf.lastBroadcastTime = time.Unix(0, 0)
	go rf.broadcastHeartBeat()
}

/*
* 支持快照之后，log不在保存所有的内容，日志编号
* 为快照和log的长度
 */
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) lastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastIncludedTerm // for snapshot
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

// 日志index转化成log数组下标，pos<index
func (rf *Raft) index2LogPos(index int) int {
	return index - rf.lastIncludedIndex - 1
}

// 超设定容量进行压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}
