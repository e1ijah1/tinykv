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
	return &RaftLog{
		storage: storage,
		entries: make([]pb.Entry, 1),
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
	if int(l.stabled) >= len(l.entries) {
		return nil
	}
	return l.entries[l.stabled+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied+1 : l.committed+1]
}

func (l *RaftLog) firstIndex() uint64 {
	i, err := l.storage.FirstIndex()
	if err != nil {
		log.Panicf("get first index from storage error: %v", err)
	}
	return i
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
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

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIdx := l.firstIndex()-1
	if i < dummyIdx || i > l.LastIndex() {
		return 0, nil
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
		case after == l.offset + uint64(len(l.entries)):
			l.entries = append(l.entries, ents...)
		// advance offset
		case after <= l.offset:
			l.offset=after
			l.entries=ents
		default:
			l.entries=append([]pb.Entry{}, l.entries[0:after-l.offset]...)
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
