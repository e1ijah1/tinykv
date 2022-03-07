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
	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry
	// unstable.entries[i] has raft log position i+unstable.offset.
	// Note that unstable.offset may be less than the highest log position in storage;
	//this means that the next write to storage
	// might need to truncate the log before persisting unstable.entries.
	offset uint64

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstLogIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	fi, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("get first index from storage err %v", err)
	}
	li, err := storage.LastIndex()
	if err != nil {
		log.Panicf("get last index from storage err %v", err)
	}
	entries, err := storage.Entries(fi, li+1)
	if err != nil {
		log.Panicf("get entries from storage err %v", err)
	}

	return &RaftLog{
		storage:       storage,
		committed:     fi - 1,
		applied:       fi - 1,
		stabled:       li,
		entries:       entries,
		firstLogIndex: fi,
		// offset 表示 unstable log index 与 entries array 中的 index 的差值
		// entries[i] 的 index 为 i + offset
		offset: 1,
	}
}

func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && !IsEmptySnap(l.pendingSnapshot)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// remove stabled logs
	firstStabledIndex, _ := l.storage.FirstIndex()
	for len(l.entries) > 0 {
		if l.firstLogIndex >= firstStabledIndex {
			break
		}

		l.entries = l.entries[1:]
		if len(l.entries) < 1 {
			l.firstLogIndex = 0
		} else {
			l.firstLogIndex = l.entries[0].Index
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	begin := l.stabled - l.firstLogIndex + 1
	if begin <= uint64(len(l.entries)) {
		return l.entries[begin:]
	}
	return []pb.Entry{}
}

func (l *RaftLog) hasNextEnts() bool {
	// check if has committed but not applied entries
	offset := max(l.applied+1, l.firstIndex())
	return l.committed > offset
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).
	ents := make([]pb.Entry, 0, l.committed-l.applied)
	for _, ent := range l.entries {
		if ent.Index > l.applied && ent.Index <= l.committed {
			ents = append(ents, ent)
		}
	}
	return ents
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("raftlog: applied out of range, committed: %d applied: %d, appliedto: %d", l.committed, l.applied, i)
	}
	l.applied = i
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) stableTo(i, t uint64) {
	gt, ok := l.unstableTerm(i)
	if !ok {
		return
	}
	if gt == t && i >= l.offset {
		l.entries = l.entries[i-l.offset+1:]
		l.offset = i + 1
		l.shrinkUnstableEntriesArray()
		l.stabled = 0
	}
}

// shrink underlying array size
func (l *RaftLog) shrinkUnstableEntriesArray() {
	const lenMul = 2
	if len(l.entries) == 0 {
		l.entries = nil
	} else if len(l.entries)*lenMul < cap(l.entries) {
		newEntries := make([]pb.Entry, len(l.entries))
		copy(newEntries, l.entries)
		l.entries = newEntries
	}
}

func (l *RaftLog) firstIndex() uint64 {
	if i, ok := l.firstUnstableIndex(); ok {
		return i
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		log.Panicf("get first index from storage error: %v", err)
	}
	return i
}

func (l *RaftLog) firstUnstableIndex() (uint64, bool) {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1, true
	}
	return 0, false
}

func (l *RaftLog) lastUnstableIndex() (uint64, bool) {
	if ll := len(l.entries); ll != 0 {
		return l.offset + uint64(ll) - 1, true
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index, true
	}
	return 0, false
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	var index uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		index = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}

	i, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("get last index from storage error: %v", err)
	}
	return max(index, i)
}

func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("get last term error: %v", err)
	}
	return t
}

func (l *RaftLog) unstableTerm(i uint64) (uint64, bool) {
	// Your Code Here (2A).
	if i < l.offset {
		if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, true
		}
		return 0, false
	}
	last, ok := l.lastUnstableIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}
	if i-l.offset < uint64(len(l.entries)) {
		return l.entries[i-l.offset].Term, true
	}
	return 0, false
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
	}
	for _, ent := range l.entries {
		if ent.Index == i {
			return ent.Term, nil
		}
	}
	return l.storage.Term(i)
}

func (l *RaftLog) appendEntriesWithTerm(ents []*pb.Entry, term uint64, pendingConfIndex *uint64) {
	for _, ent := range ents {
		if ent.EntryType == pb.EntryType_EntryConfChange {
			if *pendingConfIndex != None {
				continue
			}
			*pendingConfIndex = ent.Index
		}

		l.entries = append(l.entries, pb.Entry{
			EntryType: ent.EntryType,
			Term:      term,
			Index:     l.LastIndex() + 1,
			Data:      ent.Data,
		})
	}
	return
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) < 1 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after %d is out of range [committed %d]", after, l.committed)
	}

	// handle log conflict in raft append method
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}

	return l.LastIndex()
}

func (l *RaftLog) deleteFromIndex(index uint64) {
	idx := index - l.firstLogIndex
	l.entries = l.entries[:idx]
	lastLogIndex := l.LastIndex()
	l.committed = min(l.committed, lastLogIndex)
	l.applied = min(l.applied, lastLogIndex)
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) getEntries(i uint64) ([]*pb.Entry, error) {
	n := len(l.entries)
	if i > l.LastIndex() || n < 1 {
		return nil, nil
	}
	beginIndex := i - l.firstLogIndex
	if beginIndex < 0 {
		beginIndex = 0
	}
	ret := make([]*pb.Entry, 0, n-int(beginIndex))
	for j := int(beginIndex); j < n; j++ {
		ent := l.entries[j]
		ret = append(ret, &pb.Entry{
			EntryType: ent.EntryType,
			Term:      ent.Term,
			Index:     ent.Index,
			Data:      ent.Data,
		})
	}
	return ret, nil
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) restore(snap *pb.Snapshot) {
	l.committed = snap.Metadata.Index
	l.offset = snap.Metadata.Index + 1
	l.entries = make([]pb.Entry, 0)
	l.pendingSnapshot = snap
}

func (l *RaftLog) resetAllIndex(index uint64) {
	l.committed = index
	l.applied = index
	l.stabled = index
	l.firstLogIndex = index + 1
}
