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
	"log"

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
		storage:   storage,
		committed: fi - 1,
		applied:   fi - 1,
		stabled:   li,
		entries:   entries,
		// offset 表示 unstable log index 与 entries array 中的 index 的差值
		// entries[i] 的 index 为 i + offset
		offset: 1,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled >= uint64(len(l.entries)) {
		return []pb.Entry{}
	}
	return l.entries[l.stabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).
	offset := max(l.applied+1, l.firstIndex())
	if l.committed+1 > offset {
		ents, err := l.slice(offset, l.committed+1)
		if err != nil {
			log.Panicf("unexpected error when slice entries %v", err)
		}
		return ents
	}
	return nil
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
	if i, ok := l.lastUnstableIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("get last index from storage error: %v", err)
	}
	return i
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
	dummyIdx := l.firstIndex() - 1
	if i < dummyIdx || i > l.LastIndex() {
		return 0, nil
	}

	if t, ok := l.unstableTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	log.Panicf("get term from storage error: %v", err)
	return 0, nil
}

func (l *RaftLog) isUpToDate(lastIdx, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lastIdx >= l.LastIndex())
}

func (l *RaftLog) truncateUnstableAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	case after == l.offset+uint64(len(l.entries)):
		l.entries = append(l.entries, ents...)
	// advance offset
	case after <= l.offset:
		l.offset = after
		l.entries = ents
	default:
		l.entries = append([]pb.Entry{}, l.entries[0:after-l.offset]...)
		l.entries = append(l.entries, ents...)
	}
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) < 1 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after %d is out of range [committed %d]", after, l.committed)
	}
	l.truncateUnstableAndAppend(ents)
	return l.LastIndex()
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
	if i > l.LastIndex() {
		return nil, nil
	}
	ents, err := l.slice(i, l.LastIndex()+1)
	if err != nil {
		return nil, err
	}
	ret := make([]*pb.Entry, 0, len(ents))
	for _, e := range ents {
		ret = append(ret, &pb.Entry{
			EntryType: e.EntryType,
			Term:      e.Term,
			Index:     e.Index,
			Data:      e.Data,
		})
	}
	return ret, nil
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}

	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry

	// get entries from storage
	if lo < l.offset {
		storedEnts, err := l.storage.Entries(lo, min(l.offset, hi))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			log.Panicf("entries[%d:%d] unavailable from storage", lo, min(l.offset, hi))
		} else if err != nil {
			log.Panicf("unexpected error %v", err)
		}

		if uint64(len(storedEnts)) < min(l.offset, hi)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}

	if hi > l.offset {
		unstable := l.unstableSlice(max(lo, l.offset), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}

	return ents, nil
}

func (l *RaftLog) unstableSlice(lo uint64, hi uint64) []pb.Entry {
	if lo > hi {
		log.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := l.offset + uint64(len(l.entries))
	if lo < l.offset || hi > upper {
		log.Panicf("unstable.slice[%d:%d] out of bound [%d:%d]", lo, hi, l.offset, upper)
	}
	return l.entries[lo-l.offset : hi-l.offset]
}

func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}

	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if hi > fi+length {
		log.Panicf("slice[%d,%d] out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
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

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			if e.Index <= l.LastIndex() {
				log.Printf("found conflict at index %d [existing term: %d, conflicting term: %d]",
					e.Index, l.zeroTermOnErrCompacted(l.Term(e.Index)), e.Term)
			}
			return e.Index
		}
	}
	return 0
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, entsPtr ...*pb.Entry) (lastnewi uint64, ok bool) {
	ents := make([]pb.Entry, 0, len(entsPtr))
	for _, e := range entsPtr {
		ents = append(ents, *e)
	}

	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			if ci-offset > uint64(len(ents)) {
				log.Panicf("index, %d, is out of range [%d]", ci-offset, len(ents))
			}
			after := ents[0].Index
			if after-1 < l.committed {
				log.Panicf("after %d is out of range [committed %d]", after, l.committed)
			}
			switch {
			case after == l.offset+uint64(len(l.entries)):
				l.entries = append(l.entries, ents...)
			case after <= l.offset:
				l.offset = after
				l.entries = ents
				l.stabled = 0
			default:
				// 5.3 This means that conﬂicting entries in follower logs will be overwritten with entries from the leader’s log.
				l.stabled = after - l.offset
				l.entries = append([]pb.Entry{}, l.entries[0:after-l.offset]...)
				l.entries = append(l.entries, ents...)
			}
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
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
