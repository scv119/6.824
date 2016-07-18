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
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	LeaderID         int
	Timeout          time.Duration
	StateChannel     chan int
	HeartBeatChannel chan bool
	ApplyCh          chan ApplyMsg
	CurrentTerm      int
	VotedFor         int
	Log              []Log

	// Volatile state on all servers.
	CommitIndex int
	LastApplied int

	// Volatile state on leaders.
	NextIndex  []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.LeaderID == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm ||
		rf.VotedFor >= 0 && rf.VotedFor != args.CandidateID {
		return
	}

	// Grant vote if the candidate is at least as up-to-date as me.
	lastLogTerm := 0
	lastLogIndex := 0
	if len(rf.Log) > 0 {
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
		lastLogIndex = len(rf.Log)
	}
	if args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = true
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		DPrintf("Node %v -> Follower, Term: %v, Reject\n", rf.me, rf.CurrentTerm)
		reply.Success = false
		return
	}

	// Heartbeat.
	if len(args.Entries) != 0 {

		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
		if args.PrevLogIndex > 0 && (len(rf.Log) < args.PrevLogIndex || rf.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			DPrintf("Node %v -> Follower, Term: %v, Reject\n", rf.me, rf.CurrentTerm)
			reply.Success = false
			return
		}

		// Appending Logs.
		logIndex := args.PrevLogIndex
		for entriesIndex := 0; entriesIndex < len(args.Entries); entriesIndex++ {
			if logIndex < len(rf.Log) {
				// If an existing entry conficts with a new one, delete the existing entry and all that follow it.
				if rf.Log[logIndex].Term != args.Entries[entriesIndex].Term {
					rf.Log = rf.Log[:logIndex-1]
				}
			}

			if logIndex >= len(rf.Log) {
				rf.Log = append(rf.Log, args.Entries[entriesIndex])
			}

			logIndex++
		}
	}

	toBeCommit := len(rf.Log)
	if args.LeaderCommit < toBeCommit {
		toBeCommit = args.LeaderCommit
	}
	for {
		if rf.CommitIndex >= toBeCommit {
			break
		}

		rf.CommitIndex++

		rf.ApplyCh <- ApplyMsg{
			Index:   rf.CommitIndex,
			Command: rf.Log[rf.CommitIndex-1].Command,
		}
	}

	// Since args.Term >= rf.CurrentTerm, this works for both follower and leaders as
	// for leader, args.Term shoud > rf.CurrentTerm.
	select {
	case rf.HeartBeatChannel <- true:
	default:
	}
	//DPrintf("Node %v -> Follower, Term: %v, OK2\n", rf.me, rf.CurrentTerm)

	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm
	reply.Success = true
	rf.LeaderID = args.LeaderID
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	currentTerm, isLeader := rf.GetState()
	if !isLeader {
		return index, currentTerm, isLeader
	}
	DPrintf("Node %v -> Leader, Term: %v, Start Command\n", rf.me, rf.CurrentTerm)
	rf.Log = append(rf.Log, Log{
		Term:    rf.CurrentTerm,
		Command: command,
	})
	logIndex := len(rf.Log)
	rf.MatchIndex[rf.me] = logIndex
	rf.LastApplied = logIndex
	return logIndex, currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.Timeout = time.Duration(100 + rand.Intn(50))
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []Log{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.LeaderID = -1
	rf.StateChannel = make(chan int)
	rf.HeartBeatChannel = make(chan bool)
	rf.ApplyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serve()
	rf.StateChannel <- Follower

	return rf
}

func (rf *Raft) serve() {
	for {
		switch <-rf.StateChannel {
		case Leader:
			go rf.asLeader()
			break
		case Follower:
			go rf.asFollower()
			break
		default:
			go rf.asCandidate()
		}
	}
}

func (rf *Raft) asFollower() {
	DPrintf("Node %v -> Follower, Term: %v\n", rf.me, rf.CurrentTerm)
	rf.VotedFor = -1
	ticker := time.NewTicker(time.Millisecond * rf.Timeout)
	go func() {
		timeout := true
		for {
			select {
			case <-ticker.C:
				if timeout {
					ticker.Stop()
					rf.StateChannel <- Candidate
					return
				}
				timeout = true
			case <-rf.HeartBeatChannel:
				timeout = false
			}
		}
	}()
}

func (rf *Raft) asCandidate() {
	rf.CurrentTerm++
	DPrintf("Node %v -> Candidate, Term: %v\n", rf.me, rf.CurrentTerm)
	rf.VotedFor = rf.me
	grantedNumber := 1
	elected := false
	expired := false
	usurped := false

	// Sending Request vote RPC to all other servers.
	for tmpIdx := range rf.peers {
		if tmpIdx == rf.me {
			continue
		}
		idx := tmpIdx
		go func() {
			lastLogTerm := 0
			if len(rf.Log) > 0 {
				lastLogTerm = rf.Log[len(rf.Log)-1].Term
			}

			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: len(rf.Log),
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			if rf.sendRequestVote(idx, args, &reply) {

				if usurped || expired || elected {
					return
				}

				if reply.VoteGranted {
					DPrintf("Candidate %v request vote from %v succeed , Term: %v\n", rf.me, idx, rf.CurrentTerm)
					grantedNumber++
				} else {
					DPrintf("Candidate %v request vote from %v failed , Term: %v\n", rf.me, idx, rf.CurrentTerm)
				}

				if grantedNumber >= len(rf.peers)/2+1 {
					elected = true
					rf.StateChannel <- Leader
				}
			}
		}()
	}

	timer := time.NewTimer(time.Millisecond * rf.Timeout)

	go func() {
		select {
		case <-timer.C:
			if elected {
				return
			}
			expired = true
			rf.StateChannel <- Follower
		case <-rf.HeartBeatChannel:
			usurped = true
			rf.StateChannel <- Follower
		}
	}()
}

func (rf *Raft) asLeader() {
	DPrintf("Node %v -> Leader, Term: %v\n", rf.me, rf.CurrentTerm)
	rf.LeaderID = rf.me
	usurped := false

	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = len(rf.Log) + 1
		rf.MatchIndex[i] = 0
	}

	go func() {
		for tmpIdx := range rf.peers {
			idx := tmpIdx
			go func() {
				ticker := time.NewTicker(time.Millisecond * time.Duration(10))
				for range ticker.C {
					if usurped {
						break
					}

					if idx == rf.me {
						return
					}

					entries := []Log{}
					prevLogIndex := 0
					prevLogTerm := 0

					if rf.LastApplied >= rf.NextIndex[idx] {
						DPrintf("Node %v -> Leader, Term: %v, deciding %v, nextIdx %v, LastApplied %v \n", rf.me, rf.CurrentTerm, idx,
							rf.NextIndex[idx], rf.LastApplied)
						prevLogIndex = rf.NextIndex[idx] - 1
						if prevLogIndex > 0 {
							prevLogTerm = rf.Log[prevLogIndex-1].Term
						}
						entries = rf.Log[prevLogIndex:]
					} else {
						DPrintf("Node %v -> Leader, Term: %v, heart beat to %v \n", rf.me, rf.CurrentTerm, idx)
					}

					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderID:     rf.me,
						LeaderCommit: rf.CommitIndex,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
					}
					reply := AppendEntriesReply{}
					if usurped {
						return
					}
					if rf.sendAppendEntries(idx, args, &reply) {
						//DPrintf("Leader %v , Term: %v, HeartBeat from %v: %v, %v\n",
						//	rf.me, rf.CurrentTerm, idx, reply.Term, reply.Success)

						if !reply.Success {
							if reply.Term > rf.CurrentTerm {
								usurped = true
								rf.CurrentTerm = reply.Term
								rf.HeartBeatChannel <- true
							} else if rf.NextIndex[idx] > 1 {

								DPrintf("Node %v -> Leader, Term: %v, reply from %v: %v \n", rf.me, rf.CurrentTerm, idx, reply.Success)
								rf.NextIndex[idx]--
							}
						} else if len(entries) > 0 {
							rf.MatchIndex[idx] = rf.LastApplied
							rf.NextIndex[idx] = rf.LastApplied + 1

							DPrintf("Node %v -> Leader, Term: %v, Applied by %v, nextIdx %v, LastApplied %v \n", rf.me, rf.CurrentTerm, idx,
								rf.NextIndex[idx], rf.LastApplied)
						}
					}
				}
				ticker.Stop()
			}()
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(10))
		for range ticker.C {
			if usurped {
				break
			}

			for tmpIdx := range rf.peers {
				idx := tmpIdx
				go func() {
					if idx == rf.me {
						return
					}

					args := AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderID:     rf.me,
						LeaderCommit: rf.CommitIndex,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      []Log{},
					}
					reply := AppendEntriesReply{}
					if usurped {
						return
					}
					if rf.sendAppendEntries(idx, args, &reply) {
						if !reply.Success && reply.Term > rf.CurrentTerm {
							usurped = true
							rf.CurrentTerm = reply.Term
							rf.HeartBeatChannel <- true
						}
					}
				}()
			}
		}
		ticker.Stop()
	}()

	go func() {
		<-rf.HeartBeatChannel
		usurped = true
		rf.StateChannel <- Follower
	}()

	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(10))
		for range ticker.C {
			if usurped {
				break
			}

			matchIndexes := append(rf.MatchIndex[:rf.me], rf.MatchIndex[rf.me:]...)
			sort.Sort(sort.Reverse(sort.IntSlice(matchIndexes)))
			majority := matchIndexes[len(rf.peers)/2]
			if majority > rf.CommitIndex && rf.Log[majority-1].Term == rf.CurrentTerm {
				for ; rf.CommitIndex < majority; rf.CommitIndex++ {
					DPrintf("Node %v, Commit Log %v \n", rf.me, rf.CommitIndex+1)
					rf.ApplyCh <- ApplyMsg{
						Index:   rf.CommitIndex + 1,
						Command: rf.Log[rf.CommitIndex].Command,
					}
				}

				DPrintf("Node %v, Commit Index %v \n", rf.me, rf.CommitIndex)
			}
		}
		ticker.Stop()
	}()
}
