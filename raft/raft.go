// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// region id, for debug
	RegionID uint64

	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next   uint64
	lastVisitTime int64
}

func (p *Progress) maybeUpdate(n uint64) bool {
	updated := false
	if n > p.Match {
		p.Match = n
		updated = true
	}
	p.Next = max(p.Next, n+1)
	return updated
}

func (p *Progress) maybeDecrTo(rejected, matchHint uint64) bool {
	if p.Next-1 != rejected {
		return false
	}
	p.Next = max(min(rejected, matchHint+1), 1)
	return false
}

type Raft struct {
	id uint64
	// region id, for debug
	regionID uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomizedElectionTimeout int
	leaderLeaseTimeout        int64
	leaderAliveTimeout        int
	leaderAliveElapsed        int
	mu                        sync.RWMutex
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:                 c.ID,
		electionTimeout:    c.ElectionTick,
		heartbeatTimeout:   c.HeartbeatTick,
		Prs:                make(map[uint64]*Progress),
		votes:              make(map[uint64]bool),
		Lead:               None,
		RaftLog:            newLog(c.Storage),
		leaderLeaseTimeout: int64(12 * c.ElectionTick),
		leaderAliveTimeout: 3 * c.HeartbeatTick,
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		log.Panicf("node %d get init state failed, %v", r.id, err)
	}
	// reset from storage
	r.Vote = hs.Vote
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	if r.Term == 0 {
		r.Term = r.RaftLog.LastTerm()
	}

	if c.peers == nil {
		c.peers = cs.Nodes
	}

	for _, pid := range c.peers {
		r.Prs[pid] = &Progress{
			Match: 0,
			Next:  1,
		}
		if pid == r.id {
			r.Prs[pid].Match = r.RaftLog.committed
			r.Prs[pid].Next = r.Prs[pid].Match + 1
		}
	}

	r.resetRandomElectionTimeout()
	r.State = StateFollower
	return r
}

func (r *Raft) ID() uint64 {
	return r.id
}

func (r *Raft) IsLeader() bool {
	return r.State == StateLeader
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.Prs[to]
	m := pb.Message{To: to}

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.getEntries(pr.Next)
	if len(ents) < 1 && !sendIfEmpty {
		return false
	}

	if errt != nil || erre != nil {
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("node %d send snapshot to %d failed, %v", r.id, to, err)
				return false
			}
			log.Panicf("node %d get snapshot failed, %v", r.id, err)
		}
		if IsEmptySnap(&snapshot) {
			log.Panicf("node %d get empty snapshot", r.id)
		}
		m.Snapshot = &snapshot
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.RaftLog.committed
	}
	r.send(m)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	switch r.State {
	case StateCandidate:
		fallthrough
	case StateFollower:
		r.electionElapsed++

		if r.electionElapsed >= r.randomizedElectionTimeout && r.State != StateLeader {
			r.electionElapsed = 0
			err := r.Step(pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgHup})
			if err != nil {
				log.Debugf("node %d step msgHup failed, %v", r.id, err)
			}
		}
	case StateLeader:
		// todo leader transferee
		r.heartbeatElapsed++
		r.electionElapsed++
		r.leaderAliveElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout && r.State == StateLeader {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				log.Debugf("node %d step msgBeat failed, %v", r.id, err)
			}
		}
		if r.leaderAliveElapsed >= r.leaderAliveTimeout && r.State == StateLeader {
			r.leaderAliveElapsed = 0

			curMs := currentMs()
			alives := 0
			for id := range r.Prs {
				if id == r.id {
					alives++
					continue
				}
				if r.checkFollowerActive(id, curMs) {
					alives++
				}
			}
			if alives < r.quorum() {
				log.Errorf("[region %d] node %d leader alive timeout, and has not enough alive followers, step down to follower",
					r.regionID, r.id)
				r.becomeFollower(r.Term, None)
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)

	r.Lead = lead
	r.State = StateFollower

	log.Debugf("node %d became follower at term %d, vote: %d", r.id, r.Term, r.Vote)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		log.Panicf("node %d's invalid state %s before become candidate", r.id, r.State)
		return
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id

	log.Debugf("node %d became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		log.Panicf("node %d's state must be candidate before become leader", r.id)
		return
	}
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	noopEnt := &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	}
	li := r.RaftLog.append(noopEnt)
	r.Prs[r.id].maybeUpdate(li)
	r.maybeCommit()
	log.Debugf("node %d become leader at term %d, try send noop log to all followers, index: %d, term: %d",
		r.id, r.Term, noopEnt.Index, noopEnt.Term)

	log.Debugf("node %d became leader at term %d", r.id, r.Term)
	r.bcastAppend()
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomElectionTimeout()

	r.Lead = None

	r.votes = map[uint64]bool{}

	lastIdx := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id].Next = lastIdx + 1
		r.Prs[id].Match = 0
		r.updateLastVisit(id)
		if id == r.id {
			r.Prs[id].Match = lastIdx
		}
	}
}

func (r *Raft) advanceCommitForLeader() {
	if r.State != StateLeader {
		return
	}
	maxCommit := r.RaftLog.committed

	// calc committed index
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		cnt := 0
		for _, pr := range r.Prs {
			if pr.Match >= i {
				cnt++
			}
		}
		term, _ := r.RaftLog.Term(i)
		if r.Term == term && cnt >= r.quorum() {
			maxCommit = i
		}
	}
	if maxCommit > r.RaftLog.committed {
		r.RaftLog.committed = maxCommit
		log.Debugf("node ld %d %d advance commit to %d", r.id, r.Term, r.RaftLog.committed)
		r.bcastAppend()
	}
}

func (r *Raft) maybeCommit() {
	mci := r.committedIndex()
	r.RaftLog.maybeCommit(mci, r.Term)
}

// calc committed index for marjority node
func (r *Raft) committedIndex() uint64 {
	n := len(r.Prs)
	srt := make([]uint64, n)

	i := n - 1
	for _, pr := range r.Prs {
		srt[i] = pr.Match
		i--
	}
	insertionSort(srt)

	pos := n - (n/2 + 1)
	return srt[pos]
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Debugf("node %d (%d %v, vote: %d)receive msg %+v",
		r.id, r.Term, r.State, r.Vote, m)

	if m.Term > r.Term {
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateLeader:
		r.stepLeader(m)
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	}
	return nil
}

func (r *Raft) updateLastVisit(peer uint64) {
	r.Prs[peer].lastVisitTime = currentMs()
}

func (r *Raft) checkFollowerActive(to uint64, curMs int64) bool {
	return (curMs - r.Prs[to].lastVisitTime) < r.leaderLeaseTimeout
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.updateLastVisit(m.From)
		pr := r.Prs[m.From]
		if pr == nil {
			log.Errorf("node %d not found in prs", m.From)
			return
		}
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	}
	return
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.send(m)
		}
	}
	return
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.IsLeader() {
			r.advanceCommitForLeader()
		}
	}

	r.PendingConfIndex = None
}

func (r *Raft) poll(id uint64, v bool) (granted int, rejected int) {

	log.Debugf("node %x received vote %v from %x at term %d", r.id, v, id, r.Term)

	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	// TallyVotes

	for id := range r.Prs {
		v, ok := r.votes[id]
		if !ok {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	return
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) committedEntryInCurrentTerm() bool {
	return r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(r.RaftLog.committed)) == r.Term
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		r.RaftLog.appliedTo(newApplied)
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// -------- sending func -------
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	nextIdx := r.Prs[to].Next
	preLogIndex := uint64(0)
	if nextIdx > 0 {
		preLogIndex = nextIdx - 1
	}

	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		r.sendSnapshot(to)
		return
	}

	entries, _ := r.RaftLog.getEntries(nextIdx)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Commit:  r.RaftLog.committed,
		Entries: entries,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
	})
}

func (r *Raft) sendAppendResp(to, lastIdx uint64, reject bool) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		Index:   lastIdx,
		Reject:  reject,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	})
}
func (r *Raft) sendHeartbeatResp(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
	})
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
	})
}

func (r *Raft) sendRequestVote(to, lastIndex, lastTerm uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
	})
}

func (r *Raft) sendRequestVoteResp(to uint64, reject bool) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) sendSnapshot(to uint64) bool {
	if !r.checkFollowerActive(to, currentMs()) {
		return false
	}
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Errorf("node %d failed to get snapshot from storage error %v", r.id, err)
		return false
	}
	r.send(pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		Snapshot: &snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
	return true
}

func (r *Raft) send(m pb.Message) {
	// set from of msg using current node id
	if m.From == None {
		m.From = r.id
	}

	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			log.Panicf("term should be set when sending %s", m.MsgType)
		}
	} else {
		// set term of msg using current term
		// except MsgVote & MsgVoteResp & MsgPropose
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// --------- handle funcs --------
func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != None {
		log.Errorf("node %d [term %d] transfer leadership to %d is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
		return
	}

	// local append
	r.RaftLog.appendEntriesWithTerm(m.Entries, r.Term, &r.PendingConfIndex)
	// bcast
	r.bcastAppend()

	li := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = li
	r.Prs[r.id].Next = li + 1
	r.maybeCommit()
}

func (r *Raft) campaign() {
	if r.State == StateLeader {
		log.Debugf("node %d is already leader", r.id)
		return
	}
	r.becomeCandidate()

	r.votes[r.id] = true
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// send vote reqs
	lastIdx, lastTerm := r.RaftLog.LastIndex(), r.RaftLog.LastTerm()
	for id := range r.Prs {
		if id != r.id {
			r.sendRequestVote(id, lastIdx, lastTerm)
		}
	}
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	// ignore stale request vote response
	if m.Term < r.Term || m.Term > r.Term {
		return
	}
	gr, rj := r.poll(m.From, !m.Reject)
	log.Debugf("node %x has received %d votes and %d vote rejections", r.id, gr, rj)
	if gr >= r.quorum() {
		r.becomeLeader()
	} else if rj >= r.quorum() {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResp(m.From, true)
		return
	}

	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResp(m.From, true)
		return
	}

	lastIdx := r.RaftLog.LastIndex()
	lastTerm := r.RaftLog.LastTerm()
	if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIdx) {
		r.electionElapsed = 0
		r.Vote = m.From
		r.sendRequestVoteResp(m.From, false)
		return
	}
	r.sendRequestVoteResp(m.From, true)
}

func (r *Raft) handleAppendResp(m pb.Message) {
	pr := r.Prs[m.From]
	// if reject then retry
	if m.Reject {
		nextProbeIdx := m.Index
		if m.LogTerm > 0 {
			nextProbeIdx = r.RaftLog.findConflictByTerm(m.Index, m.LogTerm)
		}
		pr.Next = max(nextProbeIdx, 1)
		r.sendAppend(m.From)
		return
	}
	// accept, update index
	r.updateLastVisit(m.From)
	if m.Index > pr.Match {
		pr.Next = m.Index + 1
		pr.Match = m.Index

		r.advanceCommitForLeader()
	}

	if r.leadTransferee == m.From {
		if pr.Match >= r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
			r.leadTransferee = None
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	if m.Term < r.Term {
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return
	}
	// update state of follower
	r.electionElapsed = 0
	r.Lead = m.From

	// check log consistency
	localTerm, _ := r.RaftLog.Term(m.Index)
	if m.LogTerm != localTerm {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Term:    r.Term,
			LogTerm: localTerm,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}

	// handle log conflicts
	for i, ent := range m.Entries {
		if ent.Index < r.RaftLog.firstLogIndex {
			continue
		} else if r.RaftLog.firstLogIndex <= ent.Index && ent.Index <= r.RaftLog.LastIndex() {
			localTerm, _ := r.RaftLog.Term(ent.Index)
			if ent.Term != localTerm {
				r.RaftLog.deleteFromIndex(ent.Index)
				r.RaftLog.append(ent)
				r.RaftLog.stabled = min(r.RaftLog.stabled, ent.Index-1)
			}
		} else {
			r.RaftLog.append(m.Entries[i:]...)
			break
		}
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.commitTo(min(m.Commit, m.Index+uint64(len(m.Entries))))
	}
	r.sendAppendResp(m.From, r.RaftLog.LastIndex(), false)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	lastIdx := m.Snapshot.Metadata.Index
	if lastIdx <= r.RaftLog.committed {
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return
	}
	r.becomeFollower(m.Term, m.From)

	r.RaftLog.resetAllIndex(lastIdx)
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.entries = []pb.Entry{}

	// reset peers
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}

	r.sendAppendResp(m.From, r.RaftLog.LastIndex(), false)
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if _, ok := r.Prs[m.From]; !ok {
		return
	}

	if m.From == r.id {
		return
	}

	r.leadTransferee = m.From
	if r.Prs[m.From].Match >= r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	_, ok := r.Prs[r.id]
	if !ok {
		return
	}
	r.campaign()
}
