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
	state       SeverState // 0: Follower 1: Candidate 2: Leader
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
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	// TODO

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
	// TODO
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// TODO
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("%s %d(%d) running RequestVote, call from %d(%d), res %v, votefor %d",
			rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted, rf.votedFor)
	}()
	// 1. the entry who send request is already stale
	if rf.currentTerm > args.Term {
		DPrintf("%s %d(%d) running RequestVote failed cause of stale term", rf.state, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// start a new term, can re-vote
	if rf.currentTerm < args.Term {
		//rf.changeState(Follower, args.Term)
		DPrintf("%s %d(%d) change to follower (%d) cause of stale1",
			rf.state, rf.me, rf.currentTerm, args.Term)
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// 2.
	lastLog := rf.log[len(rf.log)-1]
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		DPrintf("%s %d(%d) running RequestVote failed cause of voted or unmatched log %d(%d) != %d(%d)",
			rf.state, rf.me, rf.currentTerm, lastLog.Index, lastLog.Term, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	}
	rf.lastHBTime = time.Now()
	reply.Term = rf.currentTerm
}

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// TODO
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Entry

	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	// TODO
	Term             int
	Success          bool
	ConflictingTerm  int
	ConflictingIndex int
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("%s %d(%d) running AppendEntries, call from %d(%d), res %v log %v",
			rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, args.Entries)
	}()
	// 1. the entry who send request is already stale
	if rf.currentTerm > args.Term {
		DPrintf("%s %d(%d) running AppendEntries failed cause of stale term", rf.state, rf.me, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 2. log has conflict
	lastLog := rf.log[len(rf.log)-1]
	if lastLog.Index < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// fast catch up
		idx := lastLog.Index
		for idx > 0 && rf.log[idx].Term == args.PrevLogTerm {
			idx--
		}
		//reply.ConflictingTerm = rf.log[idx].Term
		reply.ConflictingIndex = idx + 1

		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("%s %d(%d) running AppendEntries failed cause of unmatched log->%d(%d)",
			rf.state, rf.me, rf.currentTerm, reply.ConflictingIndex, reply.ConflictingTerm)
		return
	}

	//if lastLog.Index > args.PrevLogIndex {
	//	rf.log = rf.log[:args.PrevLogIndex+1]
	//}

	if rf.state != Follower {
		rf.state = Follower
	}

	if rf.currentTerm < args.Term {
		DPrintf("%s %d(%d) receive a heartbeat with a new term %d(%d), now change to follower",
			rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	// 3.
	//if len(args.Entries) > 0 && lastLog.Index >= args.Entries[0].Index &&
	//	rf.log[args.Entries[0].Index].Term != args.Entries[0].Term {
	//	DPrintf("-------------------")
	//	rf.log = rf.log[:args.Entries[0].Index]
	//}
	if lastLog.Index > args.PrevLogIndex {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// 4.
	rf.log = append(rf.log, args.Entries...)
	//DPrintf("%s %d(%d) append log %v", rf.state, rf.me, rf.currentTerm, rf.log)
	//lastLog.Index += len(args.Entries)
	// 5.
	//DPrintf("%s %d(%d) args.LeaderCommit %d, rf.commitIndex %d", rf.state, rf.me, rf.currentTerm, args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}

	rf.lastHBTime = time.Now()
	reply.Success = true
	reply.Term = rf.currentTerm
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
	//rf.mu.Lock()
	//DPrintf("sending sendRequestVote from %d to %d", rf.me, server)
	//rf.mu.Unlock()
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
	rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	lastLog := rf.log[len(rf.log)-1]
	entry := Entry{
		Term:    rf.currentTerm,
		Index:   lastLog.Index + 1,
		Command: command,
	}
	DPrintf("%s %d(%d) append a log %v %v", rf.state, rf.me, rf.currentTerm, entry, entry.Command)

	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] += 1
	rf.nextIndex[rf.me] += 1

	DPrintf("%s %d(%d) matchIndex %v", rf.state, rf.me, rf.currentTerm, rf.matchIndex)
	DPrintf("%s %d(%d) nextIndex %v", rf.state, rf.me, rf.currentTerm, rf.nextIndex)

	return lastLog.Index + 1, rf.currentTerm, rf.state == Leader
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

	electionTimeOut := rand.Intn(300)

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// TODO
		rf.mu.Lock()
		// if time out

		if rf.state != Leader && time.Since(rf.lastHBTime) > time.Millisecond*time.Duration(250+electionTimeOut) {
			//DPrintf("if timeout.....")
			cnt := 1
			cntMu := sync.Mutex{}
			//rf.changeState(Candidate, rf.currentTerm+1)
			DPrintf("%s %d(%d) change to candidate (%d) cause of timeout\n",
				rf.state, rf.me, rf.currentTerm, rf.currentTerm+1)
			rf.state = Candidate // transitions to candidate state
			rf.currentTerm++     // new term
			rf.votedFor = rf.me  // vote for itself
			rf.lastHBTime = time.Now()
			n := len(rf.peers)
			me := rf.me
			//currentTerm := rf.currentTerm

			// ask vote to every peers
			voteTime := time.Now()
			lastLog := rf.log[len(rf.log)-1]
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLog.Index,
				LastLogTerm:  lastLog.Term,
			}

			for i := 0; i < n; i++ {

				// skip self
				if i == me {
					continue
				}
				go func(i int) {
					reply := RequestVoteReply{}
					rf.sendRequestVote(i, &args, &reply)
					rf.mu.Lock()
					if reply.VoteGranted {
						DPrintf("%s %d(%d) get the vote from %d(%d)",
							rf.state, rf.me, rf.currentTerm, i, reply.Term)
						cntMu.Lock()
						cnt++
						cntMu.Unlock()
					} else if reply.Term > rf.currentTerm {
						DPrintf("%s %d(%d) change to follower (%d) cause of stale2",
							rf.state, rf.me, rf.currentTerm, reply.Term)
						rf.state = Follower
						rf.currentTerm = args.Term
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}(i)
			}
			rf.mu.Unlock()

			// watching the change of cnt, if cnt is over half, then become leader(every 10ms)
			for rf.killed() == false {
				// if election timeout
				rf.mu.Lock()
				if rf.state != Candidate {
					DPrintf("%s %d(%d) already has leader or is leader", rf.state, rf.me, rf.currentTerm)
					rf.mu.Unlock()
					break
				}
				if voteTime.Add(300 * time.Millisecond).Before(time.Now()) {
					DPrintf("%s %d election timeout, sleep a while and re-elect", rf.state, rf.me)
					electionTimeOut = rand.Intn(300)
					rf.mu.Unlock()
					break
				}
				cntMu.Lock()
				if cnt > n/2 {
					DPrintf("%s %d(%d) become leader", rf.state, rf.me, rf.currentTerm)
					rf.state = Leader
					rf.nextIndex[rf.me] = rf.log[len(rf.log)-1].Index + 1
					go rf.heartBeat()
					cntMu.Unlock()
					rf.mu.Unlock()
					break
				}
				cntMu.Unlock()
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}

			// sleep a random time
			//randTime := rand.Intn(151)
			//randTime += 150
			//time.Sleep(time.Duration(randTime) * time.Millisecond)
		} else {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		n := len(rf.peers)
		if rf.state != Leader {
			DPrintf("%s %d(%d) is not a leader, it can not send heart beat, so break",
				rf.state, rf.me, rf.currentTerm)
			rf.mu.Unlock()
			break
		}

		lastLog := rf.log[len(rf.log)-1]

		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			preLog := rf.log[rf.nextIndex[i]-1]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm:  preLog.Term,
				LeaderCommit: rf.commitIndex,
			}
			if lastLog.Index >= rf.nextIndex[i] {
				args.Entries = append([]Entry{}, rf.log[rf.nextIndex[i]:]...)
			} else {
				args.Entries = nil
			}
			go func(i int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success && args.Entries != nil {
					rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
					rf.matchIndex[i] = args.Entries[len(args.Entries)-1].Index
				} else if !reply.Success {
					if reply.Term > rf.currentTerm {
						DPrintf("%s %d(%d) change to follower (%d) cause of stale3",
							rf.state, rf.me, rf.currentTerm, reply.Term)
						rf.state = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
					} else if reply.ConflictingIndex > 0 {
						DPrintf("%s %d(%d) rf.nextIndex[%d] %d reply.ConflictingIndex %d",
							rf.state, rf.me, rf.currentTerm, i, rf.nextIndex[i], reply.ConflictingIndex)
						rf.nextIndex[i] = reply.ConflictingIndex
					}
				}
			}(i)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) watchingCommit() {
	for rf.killed() == false {
		// if election timeout
		rf.mu.Lock()
		if rf.state == Leader {
			idxMap := make(map[int]int)
			for i := range rf.matchIndex {
				idxMap[rf.matchIndex[i]]++
			}
			DPrintf("matchIndex %v", rf.matchIndex)
			DPrintf("nextIndex %v", rf.nextIndex)

			for idx, cnt := range idxMap {
				if cnt > len(rf.peers)/2 && rf.log[idx].Term == rf.currentTerm && rf.commitIndex < idx {
					DPrintf("%s %d(%d) increase commitIndex to %d", rf.state, rf.me, rf.currentTerm, idx)
					rf.commitIndex = idx
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		applyNum := rf.commitIndex - rf.lastAppend
		if applyNum == 0 {
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		startIdx := rf.lastAppend + 1
		for i := 0; i < applyNum; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				//SnapshotValid: false,
				//Snapshot:      nil,
				//SnapshotTerm:  0,
				//SnapshotIndex: 0,
			}
			msg.Command = rf.log[startIdx+i].Command
			msg.CommandIndex = startIdx + i
			DPrintf("%s %d(%d) applying msg: %v", rf.state, rf.me, rf.currentTerm, msg)
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.lastAppend = rf.commitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) isAlive() {
	for rf.killed() == false {
		rf.mu.Lock()
		DPrintf("peer%d is alive, log %v", rf.me, rf.log)
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// changeState 5 kinds of state change just like Figure 4
//func (rf *Raft) changeState(state SeverState, term int) {
//	if rf.state == Follower && state == Candidate {
//		DPrintf("peer %d(%d) change to candidate cause of timeout\n", rf.me, rf.currentTerm)
//		rf.state = Candidate  // transitions to candidate state
//		rf.currentTerm = term // new term
//		rf.votedFor = rf.me   // vote for itself
//		return
//	}
//	if rf.state == Candidate {
//		switch state {
//		case Candidate:
//			DPrintf("peer %d(%d) already has leader (%d)", rf.me, rf.currentTerm, term)
//			rf.state = Follower
//			rf.currentTerm = term
//			rf.votedFor = -1
//		case Follower:
//			DPrintf("peer %d(%d) change to candidate cause of timeout\n", rf.me, rf.currentTerm)
//			rf.state = Candidate
//			rf.currentTerm = term
//			rf.votedFor = rf.me
//		case Leader:
//			DPrintf("peer %d(%d) become leader\n", rf.me, rf.currentTerm)
//			rf.state = Leader
//		}
//		return
//	}
//	if rf.state == Leader && state == Follower {
//		DPrintf("leader %d(%d) change to follower (%d) cause of stale", rf.me, rf.currentTerm, term)
//		rf.state = Follower
//		rf.currentTerm = term
//		rf.votedFor = -1
//	}
//}

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
	rf.lastHBTime = time.Now()
	rf.startTime = time.Now()

	// Your initialization code here (2A, 2B, 2C).
	// TODO
	rf.commitIndex = 0
	rf.lastAppend = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.log = append(rf.log, Entry{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.watchingCommit()
	go rf.applyLog()
	go rf.isAlive()

	return rf
}

//func (rf *Raft) updateLog()  {
//	for {
//		rf.mu.Lock()
//
//		rf.mu.Unlock()
//	}
//}

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
