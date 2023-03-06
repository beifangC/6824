package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_PUT    = "Put"
	OP_APPEND = "Append"
	OP_GET    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index int    // log属性
	Term  int    // log属性
	Type  string // PutAppend, Get

	Key   string
	Value string

	SeqId    int64
	ClientId int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        *Op
	committed chan byte //控制协程使用，不传数据

	wrongLeader bool // log[index]的term不一致,leader发生过变化
	ignored     bool // seqid过期

	// Get return
	keyExist bool
	value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // log容量上限，超出则进行快照压缩

	kvStore map[string]string  //kv存储
	reqMap  map[int]*OpContext // log index -> 请求操作
	seqMap  map[int64]int64    // 客户端id -> 客户端seq

	lastAppliedIndex int // 已应提交日志index
}

func newOpContext(op *Op) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err = OK

	op := &Op{
		Type:     OP_GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 请求raft
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 保存index位置操作
		kv.reqMap[op.Index] = opCtx
	}()

	// 删除index上的操作
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: // 如果提交了
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist { // key不存在
			reply.Err = ErrNoKey
		} else {
			reply.Value = opCtx.value // 返回值
		}
	case <-timer.C: // 超时
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = OK

	op := &Op{
		Type:     args.Op, // Put or Append
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 请求raft
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.reqMap[op.Index] = opCtx
	}()

	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: // 提交
		if opCtx.wrongLeader {
			reply.Err = ErrWrongLeader
		} else if !opCtx.ignored {
			//
			go kv.snapshot()
		}
	case <-timer.C: // 超时
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) snapshot() {
	if kv.killed() {
		return
	}
	var snapshot []byte
	var lastIncludedIndex int
	// 锁内dump snapshot
	func() {
		// log超过限定
		if kv.maxraftstate != -1 && kv.rf.ExceedLogSize(kv.maxraftstate) { // 这里调用ExceedLogSize不要加kv锁，否则会死锁
			// 锁内快照，离开锁通知raft处理
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvStore) // kv键值对
			e.Encode(kv.seqMap)  // 当前各客户端最大请求编号
			snapshot = w.Bytes()
			lastIncludedIndex = kv.lastAppliedIndex
			DPrintf("KVServer[%d] KVServer dump snapshot, snapshotSize[%d] lastAppliedIndex[%d]", kv.me, len(snapshot), kv.lastAppliedIndex)
			kv.mu.Unlock()
		}
	}()
	// 锁外通知raft层截断，否则有死锁
	if snapshot != nil {
		// 通知raft截断日志
		kv.rf.Snapshot(lastIncludedIndex, snapshot)
	}
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			// 如果是安装快照
			if msg.SnapshotValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if len(msg.Snapshot) == 0 { // 传来空快照
						kv.kvStore = make(map[string]string)
						kv.seqMap = make(map[int64]int64)
					} else { // 有快照
						// 反序列化快照
						r := bytes.NewBuffer(msg.Snapshot)
						d := labgob.NewDecoder(r)
						d.Decode(&kv.kvStore)
						d.Decode(&kv.seqMap)
					}
					// 已应用到哪个索引
					kv.lastAppliedIndex = msg.LastIncludedIndex
					DPrintf("KVServer[%v] installSnapshot, kvStore[%v], seqMap[%v] lastAppliedIndex[%v]", kv.me, len(kv.kvStore), len(kv.seqMap), kv.lastAppliedIndex)
				}()
			} else if msg.CommandValid { // log
				cmd := msg.Command
				index := msg.CommandIndex

				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					// 更新已经应用到的日志
					kv.lastAppliedIndex = index

					op := cmd.(*Op)
					opCtx, existOp := kv.reqMap[index]          //index -> 请求操作
					prevSeq, existSeq := kv.seqMap[op.ClientId] //id -> seqid
					kv.seqMap[op.ClientId] = op.SeqId

					if existOp { // 存在等待结果的RPC, 对比term
						if opCtx.op.Term != op.Term {
							opCtx.wrongLeader = true
						}
					}

					if op.Type == OP_PUT || op.Type == OP_APPEND {
						if !existSeq || op.SeqId > prevSeq { // 有新的操作覆盖了之前的
							if op.Type == OP_PUT { // put
								kv.kvStore[op.Key] = op.Value
							} else if op.Type == OP_APPEND { //
								if val, exist := kv.kvStore[op.Key]; exist {
									kv.kvStore[op.Key] = val + op.Value
								} else {
									kv.kvStore[op.Key] = op.Value
								}
							}
						} else if existOp {
							opCtx.ignored = true
						}
					} else { // OP_GET
						if existOp {
							opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
						}
					}
					DPrintf("KVServer[%d] applyLoop, kvStore[%v]", kv.me, len(kv.kvStore))

					// 唤醒挂起的RPC
					if existOp {
						close(opCtx.committed)
					}
				}()
			}
		}
	}
}

func (kv *KVServer) snapshotLoop() {
	for !kv.killed() {
		var snapshot []byte
		var lastIncludedIndex int
		// 锁内dump snapshot
		func() {
			// 如果raft log超过了maxraftstate大小，那么对kvStore快照下来
			if kv.maxraftstate != -1 && kv.rf.ExceedLogSize(kv.maxraftstate) { // 这里调用ExceedLogSize不要加kv锁，否则会死锁
				// 锁内快照，离开锁通知raft处理
				kv.mu.Lock()
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvStore) // kv键值对
				e.Encode(kv.seqMap)  // 客户端最大请求编号
				snapshot = w.Bytes()
				lastIncludedIndex = kv.lastAppliedIndex
				DPrintf("KVServer[%d] KVServer dump snapshot, snapshotSize[%d] lastAppliedIndex[%d]", kv.me, len(snapshot), kv.lastAppliedIndex)
				kv.mu.Unlock()
			}
		}()
		// 锁外通知raft层截断，否则有死锁
		if snapshot != nil {
			// 通知raft落地snapshot并截断日志（
			kv.rf.Snapshot(lastIncludedIndex, snapshot)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate //log数量上限

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1) // 至少1个容量，启动后初始化snapshot用
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)
	kv.lastAppliedIndex = 0

	DPrintf("KVServer[%v] start, maxrlog[%v]", kv.me, kv.maxraftstate)

	go kv.applyLoop()
	//go kv.snapshotLoop()

	return kv
}
