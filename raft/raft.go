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
	Match, Next uint64
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
	mu                        sync.RWMutex
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		Lead:             None,
		RaftLog:          newLog(c.Storage),
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

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return r.maybeSendAppend(to, true)
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

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}
	r.send(m)
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

func (r *Raft) campaign() {
	if r.State == StateLeader {
		log.Debugf("node %d is already leader", r.id)
		return
	}
	r.becomeCandidate()

	if gr, _ := r.poll(r.id, true); gr >= r.quorum() {
		r.becomeLeader()
		return
	}
	// send vote reqs
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{Term: r.Term, To: id, MsgType: pb.MessageType_MsgRequestVote,
			Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	switch r.State {
	case StateCandidate:
		fallthrough
	case StateFollower:
		r.electionElapsed++

		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
			if err != nil {
				log.Debugf("node %d step msgHup failed, %v", r.id, err)
			}
		}
	case StateLeader:
		// todo leader transferee
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				log.Debugf("node %d step msgBeat failed, %v", r.id, err)
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
	if !r.appendEntry(noopEnt) {
		log.Panicf("node %d append noop entry failed", r.id)
	}
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

	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
}

func (r *Raft) appendEntry(es ...*pb.Entry) bool {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}

	li = r.RaftLog.append(es...)
	r.Prs[r.id].maybeUpdate(li)
	r.maybeCommit()
	return true
}

func (r *Raft) maybeCommit() bool {
	mci := r.committedIndex()
	return r.RaftLog.maybeCommit(mci, r.Term)
}

// calc committed index for marjority node
func (r *Raft) committedIndex() uint64 {
	n := len(r.Prs)
	srt := make([]uint64, n)

	i := n - 1
	for _, pr := range r.Prs {
		srt[i] = uint64(pr.Match)
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
	if m.Term < r.Term && (m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend) {
		// ignore the message which has a lower term
		// r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
		return nil
	}

	if m.Term > r.Term {
		leader := None
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			leader = m.From
		}
		r.becomeFollower(m.Term, leader)
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default:
		var step stepFunc
		switch r.State {
		case StateLeader:
			step = stepLeader
		case StateCandidate:
			step = stepCandidate
		case StateFollower:
			step = stepFollower
		}
		if err := step(r, m); err != nil {
			return err
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, Index: r.RaftLog.committed, MsgType: pb.MessageType_MsgAppendResponse})
		return
	}

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
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Term: r.Term, Index: r.RaftLog.LastIndex(), Reject: false})
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}

	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			log.Panicf("term should be set when sending %s", m.MsgType)
		}
	} else {
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
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
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		log.Debugf("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.LastIndex(),
		})
	} else {
		log.Debugf("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
		})
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

type stepFunc func(r *Raft, m pb.Message) error

func stepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		if r.leadTransferee != None {
			log.Debugf("node %d [term %d] transfer leadership to %d is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}
		if len(m.Entries) < 1 {
			log.Panicf("node %d stepped empty MsgProp", r.id)
		}
		r.RaftLog.appendEntriesWithTerm(m.Entries, r.Term, &r.PendingConfIndex)

		r.bcastAppend()

		li := r.RaftLog.LastIndex()
		r.Prs[r.id].Match = li
		r.Prs[r.id].Next = li + 1
		if len(r.Prs) < 2 {
			r.RaftLog.commitTo(li)
		}
		return nil
	}

	pr := r.Prs[m.From]
	if pr == nil {
		log.Debugf("node %d no pr available for %d", r.id, m.From)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		// if reject then retry
		if m.Reject {
			nextProbeIdx := m.Index
			if m.LogTerm > 0 {
				nextProbeIdx = r.RaftLog.findConflictByTerm(m.Index, m.LogTerm)
			}
			pr.Next = max(nextProbeIdx, 1)
			r.sendAppend(m.From)

		} else {
			if pr.maybeUpdate(m.Index) {
				// update commit after majroity nodes received log
				if r.maybeCommit() {
					r.bcastAppend()
				}

				if r.maybeSendAppend(m.From, false) {
				}

				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgSnapshot:
		//todo
	case pb.MessageType_MsgTransferLeader:

	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj := r.poll(m.From, !m.Reject)
		log.Debugf("node %x has received %d votes and %d vote rejections", r.id, gr, rj)
		if gr >= r.quorum() {
			r.becomeLeader()
		} else if rj >= r.quorum() {
			r.becomeFollower(r.Term, None)
		}

	default:
		log.Debugf("node %d stepCandidate ignore msg %s", r.id, m.MsgType)
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			log.Infof("node %d no leader at term %d, dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("node %d no leader at term %d, dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		//todo
	default:
	}
	return nil
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

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
}

func (r *Raft) restore(snap *pb.Snapshot) bool {
	if snap.Metadata.Index <= r.RaftLog.committed {
		return false
	}

	if r.State != StateFollower {

		r.becomeFollower(r.Term+1, None)
		return false
	}

	found := false
	for _, id := range snap.Metadata.ConfState.Nodes {
		if id == r.id {
			found = true
			break
		}
	}
	if !found {
		return false
	}

	if r.RaftLog.matchTerm(snap.Metadata.Index, snap.Metadata.Term) {
		r.RaftLog.commitTo(snap.Metadata.Index)
		return false
	}
	r.RaftLog.restore(snap)

	pr := r.Prs[r.id]
	pr.maybeUpdate(pr.Next - 1)
	return true
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
