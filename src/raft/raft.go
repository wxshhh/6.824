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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type SeverState string

const (
	Follower  SeverState = "Follower"
	Candidate            = "Candidate"
	Leader               = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// TODO
	peerNum     int
	state       SeverState // Follower、Candidate、Leader
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []Entry    // log entries

	commitIndex int // index of highest log entry known to be committed
	lastAppend  int // index of highest log entry applied to state machine

	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	applyCh chan ApplyMsg

	lastHBTime time.Time // last heartbeat time
	leaderId   int

	startTime time.Time
}

// Entry Log entry
type Entry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("%s %d(%d) running RequestVote, call from %d(%d), res %v, votefor %d",
			rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted, rf.votedFor)
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	// RequestVote RPC rule1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("%s %d(%d) get a latest term, change to Follower",
			rf.state, rf.me, rf.currentTerm)
		rf.changeState(Follower, args.Term)
	}

	// RequestVote RPC rule2
	//DPrintf("%s %d(%d) %d > %d ?", rf.state, rf.me, rf.currentTerm, len(rf.log)-1, args.LastLogIndex)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	condition1 := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	condition2 := (args.LastLogTerm > lastLogTerm) || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm)
	//DPrintf("%s %d(%d) condition1 %v, condition2 %v", rf.state, rf.me, rf.currentTerm, condition1, condition2)
	//if !condition1 && !condition2 && args.Term > 1 {
	//	DPrintf("")
	//}
	if !(condition1 && condition2) {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.lastHBTime = time.Now()

}

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("%s %d(%d) running AppendEntries, call from %d(%d), res %v log %v",
			rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, args.Entries)
	}()

	reply.Term = rf.currentTerm
	reply.Success = true

	// AppendEntries RPC rule1
	if args.Term < rf.currentTerm {
		DPrintf("%s %d(%d) AppendEntries stale", rf.state, rf.me, rf.currentTerm)
		reply.Success = false
		return
	}

	rf.lastHBTime = time.Now()

	// AppendEntries RPC rule2
	if len(rf.log)-1 < args.PrevLogIndex ||
		len(rf.log)-1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%s %d(%d) AppendEntries conflict", rf.state, rf.me, rf.currentTerm)
		reply.Success = false
		return
	}

	// AppendEntries RPC rule3
	if args.Entries != nil && len(rf.log)-1 > args.PrevLogIndex {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// Rules for Servers - All Servers(2)
	if args.Term > rf.currentTerm {
		DPrintf("%s %d(%d) term %d > %d, change to Follower", rf.state, rf.me, rf.currentTerm, args.Term, rf.currentTerm)
		rf.changeState(Follower, args.Term)
		reply.Term = rf.currentTerm
	}

	if rf.state != Follower {
		DPrintf("%s %d(%d) is not Follower, change to Follower", rf.state, rf.me, rf.currentTerm)
		rf.changeState(Follower, 0)
	}

	// AppendEntries RPC rule4
	rf.log = append(rf.log, args.Entries...)

	// AppendEntries RPC rule5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	log := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}

	DPrintf("%s %d(%d) append log %v", rf.state, rf.me, rf.currentTerm, log)

	rf.log = append(rf.log, log)

	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me]++

	return rf.matchIndex[rf.me], rf.currentTerm, rf.state == Leader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	for !rf.killed() {
		sleepTime := time.Duration(300+rand.Intn(250)) * time.Millisecond
		time.Sleep(sleepTime)
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHBTime) > sleepTime {
			DPrintf("%s %d(%d) time out, change to Candidate", rf.state, rf.me, rf.currentTerm)
			rf.changeState(Candidate, 0)
		}
		rf.mu.Unlock()
	}
}

// changeState 5 kinds of state change just like Figure 4
func (rf *Raft) changeState(state SeverState, term int) {
	switch state {
	case Follower:
		rf.state = Follower
		if term != 0 {
			rf.currentTerm = term
			rf.votedFor = -1
		}
	case Candidate:
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.lastHBTime = time.Now()
		go rf.startRequestVote()
	case Leader:
		rf.state = Leader
		for i := 0; i < rf.peerNum; i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = len(rf.log) - 1
		go rf.startAppendEntries()
	}
}

func (rf *Raft) startRequestVote() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	cnt := 1

	for i := 0; i < rf.peerNum; i++ {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				DPrintf("%s %d(%d) sendRequestVote to %d failed", rf.state, rf.me, rf.currentTerm, i)
				return
			}
			if reply.VoteGranted {
				cnt++
			} else if !reply.VoteGranted && rf.currentTerm < reply.Term {
				DPrintf("%s %d(%d) RequestVote to a higher term, change to Follower", rf.state, rf.me, rf.currentTerm)
				rf.changeState(Follower, reply.Term)
			}
		}(i)
	}

	for rf.state == Candidate {
		if cnt > rf.peerNum/2 {
			DPrintf("%s %d(%d) get over half vote, change to Leader", rf.state, rf.me, rf.currentTerm)
			rf.changeState(Leader, 0)
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
}

func (rf *Raft) startAppendEntries() {
	for !rf.killed() {
		//DPrintf("start loop")
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("%s %d(%d) now is not Leader, stop sending AppendEntries", rf.state, rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		for i := 0; i < rf.peerNum; i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			}
			// Rules for Servers - Leaders(3)
			if len(rf.log)-1 >= rf.nextIndex[i] {
				args.Entries = append([]Entry{}, rf.log[rf.nextIndex[i]:]...)
			} else {
				args.Entries = nil
			}
			go func(i int) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !ok {
					DPrintf("%s %d(%d) sendAppendEntries to %d failed", rf.state, rf.me, rf.currentTerm, i)
					return
				}
				if reply.Success {
					// update nextIndex and matchIndex for follower
					rf.matchIndex[i] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[i])
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.commitLog(rf.matchIndex[i])
				} else if !reply.Success && rf.currentTerm < reply.Term {
					DPrintf("%s %d(%d) AppendEntries to a higher term, change to Follower", rf.state, rf.me, rf.currentTerm)
					rf.changeState(Follower, reply.Term)
				} else if !reply.Success && args.Entries != nil {
					// decrement nextIndex and retry
					DPrintf("%s %d(%d) decrement nextIndex[%d], retry %v", rf.state, rf.me, rf.currentTerm, i, reply)
					rf.nextIndex[i]--
				}
			}(i)
		}
		rf.mu.Unlock()
		//DPrintf("start sleep")
		time.Sleep(80 * time.Millisecond)
		//DPrintf("end sleep")
	}
}

func (rf *Raft) commitLog(idx int) {
	if rf.commitIndex >= idx {
		return
	}
	cnt := 0
	for i := range rf.peers {
		if rf.matchIndex[i] == idx {
			cnt++
		}
	}
	if cnt > rf.peerNum/2 && rf.log[idx].Term == rf.currentTerm {
		DPrintf("%s %d(%d) committed log %d", rf.state, rf.me, rf.currentTerm, idx)
		rf.commitIndex = idx
	}
}

func (rf *Raft) applyLog() {
	for {
		rf.mu.Lock()
		applyNum := rf.commitIndex - rf.lastAppend
		if applyNum > 0 {
			for i := rf.lastAppend + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
					//SnapshotValid: false,
					//Snapshot:      nil,
					//SnapshotTerm:  0,
					//SnapshotIndex: 0,
				}
				DPrintf("%s %d(%d) apply log %v", rf.state, rf.me, rf.currentTerm, msg)
				rf.lastAppend = i
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) isAlive() {
	for {
		rf.mu.Lock()
		DPrintf("%d is alive %v", rf.me, rf.log)
		if rf.state == Leader {
			DPrintf("matchIndex: %v", rf.matchIndex)
			DPrintf("nextIndex: %v", rf.nextIndex)
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.state = Follower
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.peerNum = len(peers)
	rf.lastHBTime = time.Now()
	rf.commitIndex = 0
	rf.lastAppend = 0
	rf.nextIndex = make([]int, rf.peerNum)
	rf.matchIndex = make([]int, rf.peerNum)
	rf.log = append(rf.log, Entry{Term: 0, Command: nil})
	for i := 0; i < rf.peerNum; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	go rf.ticker()
	go rf.isAlive()
	go rf.applyLog()

	return rf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
