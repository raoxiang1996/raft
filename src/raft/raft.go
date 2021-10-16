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
	"fmt"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	log "github.com/sirupsen/logrus"
)

const (
	heartbeatInterval         = 120 * time.Millisecond
	electionTimeoutLowerBound = 400
	electionTimeoutUpperBound = 600
	rpcTimeoutLimit           = 1000 * time.Millisecond
)

type State string

var (
	Leader    = State("Leader")
	Candidate = State("Candidate")
	Follower  = State("Follower")
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

	// 1 follower, 2 candidate, 3 leader
	state State

	// Persistent state on server
	currentTerm int
	// votedFor initial state is -1
	votedFor int
	logs     []LogEntry

	// Volatile state on server
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// follower election timeout timestamp
	electionTimeout  time.Time
	heartbeatTimeout time.Time
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntryArgs struct {
	Term         int        // leader的任期号
	LeaderId     int        // leaderID 便于进行重定向
	PrevLogIndex int        // 新日志之前日志的索引值
	PrevLogTerm  int        // 新日志之前日志的Term
	Entries      []LogEntry // 存储的日志条目 为空时是心跳包
	LeaderCommit int        // leader已经提交的日志的索引
}

type AppendEntryReply struct {
	Term        int  // 用于更新leader本身 因为leader可能会出现分区
	Success     bool // follower如果跟上了PrevLogIndex,PrevLogTerm的话为true,否则的话需要与leader同步日志
	CommitIndex int  // 用于返回与leader.Term的匹配项,方便同步日志
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) getState() State {
	var state State
	rf.mu.Lock()
	state = rf.state
	rf.mu.Unlock()
	return state
}

func (rf *Raft) setState(state State) {
	rf.state = state
}

func (rf *Raft) setNewTerm(newTerm int) {
	rf.currentTerm = newTerm
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
	// Your data here (2A, 2B).
	Term         int //候选人的任期号 2A
	CandidateId  int // 请求选票的候选人ID 2A
	LastLogIndex int // 候选人的最后日志条目的索引值 2A
	LastLogTerm  int // 候选人的最后日志条目的任期号 2A
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ServerId    int
	Term        int  // 当前任期号,便于返回后更新自己的任期号 2A
	VoteGranted bool // 候选人赢得了此张选票时为真 2A
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		//rf.mu.Lock()
		//if rf.state == Leader {
		//	rf.appendEntries(true)
		//}
		//rf.mu.Unlock()
		rf.mu.Lock()
		currentState := rf.state
		rf.mu.Unlock()
		if currentState == Leader {
			rf.appendEntries(true)
		}
		if rf.isElectionTimeout() {
			//fmt.Println("开始选举了")
			rf.leaderElection()
		}
	}
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
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
	//rf := &Raft{}
	//rf.peers = peers
	//rf.persister = persister
	//rf.me = me
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       Follower,
		votedFor:    -1,
		logs:        []LogEntry{},
		currentTerm: 0,

		// volatile state on servers
		commitIndex: 0,
		lastApplied: 0,

		// volatile state on leaders
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.logs = append(rf.logs, LogEntry{Command: nil, Term: -1})
	rf.resetElectionTimeout()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(
		time.Millisecond * time.Duration(rand.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound)+electionTimeoutLowerBound))
	//fmt.Println("重置计时器：", rf.electionTimeout)
}

func (rf *Raft) isElectionTimeout() bool {
	if time.Now().After(rf.electionTimeout) {
		log.Debugf("Server %v is election timeout, state %v, term %v, votedFor %+v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		return true
	}
	return false
}

//func (rf *Raft) isHeartBeatTimeout() bool {
//
//}

func (rf *Raft) leaderElection() {
	if rf.isElectionTimeout() {
		fmt.Println("节点", rf.me, " 发生超时")
	}
	fmt.Println("节点", rf.me, " 开始选举")

	//defer log.Debugf("[Candidate] Server %v finished", rf.me)
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	rf.mu.Unlock()
	log.Debugf("[Candidate] Server %v start election, state: %v, term: %v", rf.me, rf.state, rf.currentTerm)

	voteNums := 1
	successVoteNums := len(rf.peers)/2 + 1
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		LastLogIndex: len(rf.logs) - 1,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 多个线程发送请求
		go func(serverId int, args *RequestVoteArgs) {
			var reply RequestVoteReply
			log.Debugf("[Candidate] Server %v sendRequestVote to server %d, args: %+v", rf.me, serverId, *args)
			rf.sendRequestVote(serverId, args, &reply)
			if reply.VoteGranted {
				rf.mu.Lock()
				fmt.Println("获得一张选票")
				voteNums++
				log.Debugf("[Candidate] Server %v received vote from %v in term %v, currentVoteNums: %v, successVoteNums: %v", rf.me, reply.ServerId, rf.currentTerm, voteNums, successVoteNums)
				rf.resetElectionTimeout()
				rf.mu.Unlock()
			}
		}(i, &requestVoteArgs)
	}

	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Candidate && voteNums >= successVoteNums {
			rf.state = Leader
			nextIndex := len(rf.logs)
			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = nextIndex
			}
			rf.resetElectionTimeout()
			fmt.Println("选举成功 发送心跳")
			go rf.appendEntries(true)
			log.Infof("[Candidate] Server %v received the most vote, election success and become leader in term %v", rf.me, rf.currentTerm)

			time.Sleep(10 * time.Millisecond)
			break
		}
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Println("请求获得选票")
	//fmt.Println("候选人任期: ", args.Term, " 当前节点的任期：", rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Debugf("[RequestVote] Start: Server %v state: %v, currentTerm: %v, voteFor: %v,  args: %+v,", rf.me, rf.state, rf.currentTerm, rf.votedFor, args)

	reply.ServerId = rf.me
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	fmt.Println("候选者任期：", args.Term, "  当前节点任期：", rf.currentTerm)
	if args.Term < rf.currentTerm {
		fmt.Println("候选人任期小于当前节点任期")
		return
	}
	if args.Term > rf.currentTerm {
		fmt.Println("候选者任期大于当前节点任期")
		rf.setNewTerm(args.Term)
	}
	fmt.Println("投给的节点id：", rf.votedFor, "  候选人id", args.CandidateId)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//fmt.Println("候选人任期: ", args.Term, " 当前节点的任期：", rf.currentTerm)
		if args.Term == rf.currentTerm {
			fmt.Println("投票成功")
			if args.LastLogTerm > rf.logs[rf.commitIndex].Term || (args.LastLogTerm == rf.logs[rf.commitIndex].Term && args.LastLogIndex >= rf.commitIndex) {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.resetElectionTimeout()
			}
		}
	}
	return
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 用于发送附加日志项给其他服务器 也就是心跳包 超时时间为heartbeatTimeout
func (rf *Raft) SendAppendEntriesToAllFollwer() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntryArgs

		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1 // 当前最新的索引

		if args.PrevLogIndex >= 0 {
			//fmt.Printf("%d %d\n",args.PrevLogIndex,len(rf.logs))
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		// 当我们在Start中加入一条新日志的时候这里会在心跳包中发送出去
		if rf.nextIndex[i] < len(rf.logs) { // 刚成为leader的时候更新过 所以第一次entry为空
			args.Entries = rf.logs[rf.nextIndex[i]:] //如果日志小于leader的日志的话直接拷贝日志
		}
		args.LeaderCommit = rf.commitIndex

		go func(servernumber int, args AppendEntryArgs, rf *Raft) {
			var reply AppendEntryReply

		retry:

			if rf.state != Leader {
				return
			}
			ok := rf.sendAppendEntry(servernumber, args, &reply)
			if ok {

			} else {
				goto retry //附加日志失败的时候重新附加 这是很重要的 follower中对于附加日志项是幂等的
			}
		}(i, args, rf)
	}
}

func (rf *Raft) leaderSendEntryToFollower(serverId int, args *AppendEntryArgs) {
	var reply AppendEntryReply
	ok := rf.sendAppendEntry(serverId, *args, &reply)
	if !ok {
		return
	}
	fmt.Println("发送成功")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		// rules for leader 3.1
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			if rf.nextIndex[serverId] < next {
				rf.nextIndex[serverId] = next
			}
			if rf.matchIndex[serverId] < match {
				rf.matchIndex[serverId] = match
			}
			//rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			//rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
		}
	}
}

func (rf *Raft) appendEntries(heartbeat bool) {
	lastLog := rf.logs[len(rf.logs)-1]
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimeout()
			continue
		}
		// rules for leader 3
		if lastLog.Index > rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.logs[nextIndex-1]
			args := AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]LogEntry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.logs[nextIndex:])
			fmt.Println(args.Entries)
			go rf.leaderSendEntryToFollower(peer, &args)
		}
	}
	time.Sleep(30 * time.Millisecond)
}

func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	fmt.Println("追加日志")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.resetElectionTimeout()
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = Follower
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 { //心跳包
		if rf.lastApplied+1 <= args.LeaderCommit { //TODO len(rf.logs)-1 改为 rf.lastApplied+1
			rf.commitIndex = args.LeaderCommit
			//go rf.commitLogs() // 可能提交的日志落后与leader 同步一下日志
		}
		reply.CommitIndex = len(rf.logs) - 1
		reply.Success = true
		fmt.Println("成功收到心跳包")

	} else {
		if args.PrevLogIndex >= 0 && (len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.CommitIndex = len(rf.logs) - 1
			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex
			}
			for reply.CommitIndex >= 0 {
				if rf.logs[reply.CommitIndex].Term != args.Term {
					reply.CommitIndex--
				} else {
					break
				}
			}
			reply.Success = false
		} else {
			rf.logs = rf.logs[:args.PrevLogIndex+1] // debug: 第一次调用PrevLogIndex为-1
			rf.logs = append(rf.logs, args.Entries...)

			if rf.lastApplied+1 <= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit // 与leader同步信息
				//go rf.commitLogs()
			}

			// 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
			reply.CommitIndex = len(rf.logs) - 1
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < len(rf.logs)-1 {
					reply.CommitIndex = args.LeaderCommit
				}
			}
			reply.Success = true
		}
	}
	fmt.Println("节点", rf.me, "重置了时间 当前任期 ", rf.currentTerm)
	if rf.isElectionTimeout() {
		fmt.Println("还是超时!")
	}
	rf.resetElectionTimeout()
	return
}

// 提交日志
func (rf *Raft) commitLogs() { // 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.logs)-1 {
		log.Fatal("出现错误 : raft.go commitlogs()")
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ { //commit日志到与Leader相同
		// listen to messages from Raft indicating newly committed messages.
		// 调用过程才test_test.go -> start1函数中
		//TODO 这里为什么加1
		//rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}
