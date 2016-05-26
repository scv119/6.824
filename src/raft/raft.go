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

//import "fmt"
import "sync"
import "time"
import "labrpc"
import "math/rand"

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

const heartbeatIntervalMs int = 10

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                 sync.Mutex
	peers              []*labrpc.ClientEnd
	persister          *Persister
	me                 int // index into peers[]
	appendEntriesCount int
	timeout            time.Duration
	heartbeatChannel   chan bool
	commandChannel     chan int

	LeaderId int

	term         int
	voteFor      int
	log          []Log
	state        int
	stateChannel chan int
	// Volatile state.
	commitIndex int
	lastApplied int
	// Volatile state on leader.
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.term, rf.state == Leader
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

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	}

	reply.VoteGranted = true
	rf.voteFor = args.CandidateId
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
	reply.Success = false
	if args.Term < rf.term {
		return
	}
	rf.appendEntriesCount++
	reply.Success = true
	rf.term = args.Term

	select {
	case rf.heartbeatChannel <- true:
	default:
	}
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
	term := -1
	isLeader := rf.state == Leader

	if !isLeader {
		rf.log = append(rf.log, Log{Term: rf.term, Command: command})
		term = rf.term
		index = len(rf.log)
		select {
		// Notify that a new Command has been added into log. Noblocking
		// as the routine might be busy.
		case rf.commandChannel <- index:
		default:
		}
	}
	return index, term, isLeader
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

func (rf *Raft) BecomeFollower() {
	ticker := time.NewTicker(time.Millisecond * rf.timeout)
	go func() {
		appendEntriesCount := rf.appendEntriesCount
		for range ticker.C {
			if appendEntriesCount == rf.appendEntriesCount {
				rf.stateChannel <- Candidate
				break
			}
			appendEntriesCount = rf.appendEntriesCount
		}
		ticker.Stop()
	}()
}

func (rf *Raft) BecomeCandidate() {
	rf.term++
	rf.voteFor = rf.me
	grantedNumber := 1
	elected := false
	expired := false
	usurped := false

	// Sending Request vote RPC to all other servers.
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		node_idx := idx
		go func() {
			args := RequestVoteArgs{Term: rf.term, CandidateId: rf.me}
			reply := RequestVoteReply{}

			if rf.sendRequestVote(node_idx, args, &reply) {

				if usurped || expired || elected {
					return
				}

				if reply.VoteGranted {
					grantedNumber++
				}

				if grantedNumber >= len(rf.peers)/2+1 {
					elected = true
					rf.stateChannel <- Leader
				}
			}
		}()
	}

	timer := time.NewTimer(time.Millisecond * rf.timeout)

	go func() {
		select {
		case <-timer.C:
			if elected {
				return
			}

			expired = true
			rf.stateChannel <- Candidate
		case <-rf.heartbeatChannel:
			usurped = true

			rf.stateChannel <- Follower
		}
	}()
}

func (rf *Raft) BecomeLeader() {
	usurped := false
	for i := range rf.matchIndex {
		rf.matchIndex[i] = len(rf.log) + 1
		rf.nextIndex[i] = 0
	}

	// Goroutine that sends heartbeat.
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(10))
		for range ticker.C {
			if usurped {
				break
			}
			for idx := range rf.peers {
				nodeIdx := idx
				go func() {
					if nodeIdx == rf.me {
						return
					}
					lastLogIndex := len(rf.log)
					prevLogTerm := 0
					if lastLogIndex != 1 {
						prevLogTerm = rf.log[lastLogIndex-2].term
					}
					args := AppendEntriesArgs{
						Term:         rf.term,
						LeaderID:     rf.me,
						PrevLogIndex: lastLogIndex - 1,
						PrevLogTerm:  prevLogTerm,
						Entries:      []Log{},
						LeaderCommit: rf.commitIndex}
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(nodeIdx, args, &reply)
				}()
			}
		}
		ticker.Stop()
	}()

	// Goroutine that listens to heartbeat from new Leader.
	go func() {
		<-rf.heartbeatChannel
		usurped = true

		rf.stateChannel <- Follower
	}()

	// Goroutine that replicas log.
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		for {
			if usurped {
				break
			}
			lastLogIndex := len(rf.log)
			if lastLogIndex < rf.nextIndex[idx] {
				break
			}
			prevLogTerm := 0
			if lastLogIndex != 1 {
				prevLogTerm = rf.log[lastLogIndex-2].term
			}
			args := AppendEntriesArgs{
				Term:         rf.term,
				LeaderID:     rf.me,
				PrevLogIndex: lastLogIndex - 1,
				PrevLogTerm:  prevLogTerm,
				Entries:      []Log{rf.log[lastLogIndex-1]},
				LeaderCommit: rf.commitIndex}
			reply := AppendEntriesReply{}
			result := rf.sendAppendEntries(idx, args, reply)
			if !result {
				continue
			}
			if reply.Success {
				rf.nextIndex[idx] = lastLogIndex + 1
				rf.matchIndex[idx] = lastLogIndex
			} else {
				rf.nextIndex[idx]--
			}
		}
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
	rf.timeout = time.Duration(100 + rand.Intn(50))
	rf.appendEntriesCount = 0
	rf.term = 0
	rf.stateChannel = make(chan int)
	rf.heartbeatChannel = make(chan bool)
	rf.commandChannel = make(chan int)
	rf.matchIndex = []int{}
	rf.nextIndex = []int{}
	rf.log = []Log{}
	// Your initialization code here.

	go func() {
		for {
			rf.state = <-rf.stateChannel
			if rf.state == Follower {
				rf.BecomeFollower()
			} else if rf.state == Candidate {
				rf.BecomeCandidate()
			} else {
				rf.BecomeLeader()
			}
		}
	}()

	rf.stateChannel <- Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
