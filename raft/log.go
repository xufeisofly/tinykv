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
	"fmt"

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
	// 已经写入大多数 peer 的 storage 里，只写了 WAL
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已经应用到 storage 状态机里的
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 已经写入自己 storage 里的， WAL
	stabled uint64

	// all entries that have not yet compact.
	// 所有 unstabled 的 entries
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// entries 中第一条 entry 的 index - 1
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return &RaftLog{
		storage: storage,
	}
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("applied=%d, committed=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.applied, l.committed, l.stabled, len(l.entries))
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
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied+1 : l.committed+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if i, ok := l.unstableLastIndex(); ok {
		return i
	}
	ret, err := l.storage.LastIndex() // 按理说 l.storage.LastIndex() == l.stable
	if err != nil {
		panic(err)
	}
	return ret
}

func (l *RaftLog) unstableLastIndex() (uint64, bool) {
	if unstableL := len(l.entries); unstableL != 0 {
		return l.stabled + uint64(unstableL), true
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index, true
	}
	return 0, false
}

func (l *RaftLog) firstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) unstableTerm(i uint64) (uint64, bool) {
	// 如果 i 已经写入 storage，然而发现有 pendingSnapshot 且 index 等于 i，那么以 pendingSnapshot 为准
	if i <= l.stabled {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := l.unstableLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return l.entries[i-l.stabled-1].Term, true
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.firstIndex()-1 || i > l.LastIndex() {
		return 0, nil
	}

	if t, ok := l.unstableTerm(i); ok {
		return t, nil
	}

	return l.storage.Term(i)
}

func (l *RaftLog) LastTerm() uint64 {
	ret, _ := l.Term(l.LastIndex())
	return ret
}

func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lasti >= l.LastIndex())
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	case after <= l.offset:
		// 替换 after 后面的全部 entry
		l.offset = after
		l.entries = ents
	default:
		l.entries = append(l.entries, ents...)
	}
}

// raft log 添加 log，并返回最新的 index
func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if ents[0].Index-1 < l.committed {
		panic("ents index is out of range")
	}
	l.truncateAndAppend(ents)
	return l.LastIndex()
}
