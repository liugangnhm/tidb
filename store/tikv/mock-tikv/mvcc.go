// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv

import (
	"bytes"
	"sync"

	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

type mvccValue struct {
	startTS  uint64
	commitTS uint64
	value    []byte
}

type mvccLock struct {
	startTS uint64
	primary []byte
	value   []byte
	op      kvrpcpb.Op
}

type mvccEntry struct {
	key    []byte
	values []mvccValue
	lock   *mvccLock
}

func newEntry(key []byte) *mvccEntry {
	return &mvccEntry{
		key: key,
	}
}

func (e *mvccEntry) Clone() *mvccEntry {
	var entry mvccEntry
	entry.key = append([]byte(nil), e.key...)
	for _, v := range e.values {
		entry.values = append(entry.values, mvccValue{
			startTS:  v.startTS,
			commitTS: v.commitTS,
			value:    append([]byte(nil), v.value...),
		})
	}
	if e.lock != nil {
		entry.lock = &mvccLock{
			startTS: e.lock.startTS,
			primary: append([]byte(nil), e.lock.primary...),
			value:   append([]byte(nil), e.lock.value...),
			op:      e.lock.op,
		}
	}
	return &entry
}

func (e *mvccEntry) Less(than llrb.Item) bool {
	return bytes.Compare(e.key, than.(*mvccEntry).key) < 0
}

func (e *mvccEntry) lockErr() error {
	return &ErrLocked{
		Key:     e.key,
		Primary: e.lock.primary,
		StartTS: e.lock.startTS,
	}
}

func (e *mvccEntry) Get(ts uint64) ([]byte, error) {
	if e.lock != nil {
		if e.lock.startTS <= ts {
			return nil, e.lockErr()
		}
	}
	for _, v := range e.values {
		if v.commitTS <= ts {
			return v.value, nil
		}
	}
	return nil, nil
}

func (e *mvccEntry) Prewrite(mutation *kvrpcpb.Mutation, startTS uint64, primary []byte) error {
	if len(e.values) > 0 {
		if e.values[0].commitTS >= startTS {
			return ErrRetryable("write conflict")
		}
	}
	if e.lock != nil {
		if e.lock.startTS != startTS {
			return e.lockErr()
		}
		return nil
	}
	e.lock = &mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.GetOp(),
	}
	return nil
}

func (e *mvccEntry) checkTxnCommitted(startTS uint64) (uint64, bool) {
	for _, v := range e.values {
		if v.startTS == startTS {
			return v.commitTS, true
		}
	}
	return 0, false
}

func (e *mvccEntry) Commit(startTS, commitTS uint64) error {
	if e.lock == nil || e.lock.startTS != startTS {
		if _, ok := e.checkTxnCommitted(startTS); ok {
			return nil
		}
		return ErrRetryable("txn not found")
	}
	if e.lock.op != kvrpcpb.Op_Lock {
		e.values = append([]mvccValue{{
			startTS:  startTS,
			commitTS: commitTS,
			value:    e.lock.value,
		}}, e.values...)
	}
	e.lock = nil
	return nil
}

func (e *mvccEntry) Rollback(startTS uint64) error {
	if e.lock == nil || e.lock.startTS != startTS {
		if commitTS, ok := e.checkTxnCommitted(startTS); ok {
			return ErrAlreadyCommitted(commitTS)
		}
		return nil
	}
	e.lock = nil
	return nil
}

// MvccStore is an in-memory, multi-versioned, transaction-supported kv storage.
type MvccStore struct {
	mu   sync.RWMutex
	tree *llrb.LLRB
}

// NewMvccStore creates a MvccStore.
func NewMvccStore() *MvccStore {
	return &MvccStore{
		tree: llrb.New(),
	}
}

// Get reads a key by ts.
func (s *MvccStore) Get(key []byte, startTS uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.get(key, startTS)
}

func (s *MvccStore) get(key []byte, startTS uint64) ([]byte, error) {
	entry := s.tree.Get(newEntry(key))
	if entry == nil {
		return nil, nil
	}
	return entry.(*mvccEntry).Get(startTS)
}

// A Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}

// BatchGet gets values with keys and ts.
func (s *MvccStore) BatchGet(ks [][]byte, startTS uint64) []Pair {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var pairs []Pair
	for _, k := range ks {
		val, err := s.get(k, startTS)
		if val == nil && err == nil {
			continue
		}
		pairs = append(pairs, Pair{
			Key:   k,
			Value: val,
			Err:   err,
		})
	}
	return pairs
}

func regionContains(startKey []byte, endKey []byte, key []byte) bool {
	return bytes.Compare(startKey, key) <= 0 &&
		(bytes.Compare(key, endKey) < 0 || len(endKey) == 0)
}

// Scan reads up to limit numbers of Pairs from a key.
func (s *MvccStore) Scan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var pairs []Pair
	iterator := func(item llrb.Item) bool {
		if len(pairs) >= limit {
			return false
		}
		k := item.(*mvccEntry).key
		if !regionContains(startKey, endKey, k) {
			return false
		}
		val, err := s.Get(k, startTS)
		if val != nil || err != nil {
			pairs = append(pairs, Pair{
				Key:   k,
				Value: val,
				Err:   err,
			})
		}
		return true
	}
	s.tree.AscendGreaterOrEqual(newEntry(startKey), iterator)
	return pairs
}

func (s *MvccStore) getOrNewEntry(key []byte) *mvccEntry {
	if item := s.tree.Get(newEntry(key)); item != nil {
		return item.(*mvccEntry).Clone()
	}
	return newEntry(key)
}

// submit writes entries into the rbtree.
func (s *MvccStore) submit(ents ...*mvccEntry) {
	for _, ent := range ents {
		s.tree.ReplaceOrInsert(ent)
	}
}

// Prewrite acquires a lock on a key. (1st phase of 2PC).
func (s *MvccStore) Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64) []error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for _, m := range mutations {
		entry := s.getOrNewEntry(m.Key)
		err := entry.Prewrite(m, startTS, primary)
		s.submit(entry)
		errs = append(errs, err)
	}
	return errs
}

// Commit commits the lock on a key. (2nd phase of 2PC).
func (s *MvccStore) Commit(keys [][]byte, startTS, commitTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ents []*mvccEntry
	for _, k := range keys {
		entry := s.getOrNewEntry(k)
		err := entry.Commit(startTS, commitTS)
		if err != nil {
			return err
		}
		ents = append(ents, entry)
	}
	s.submit(ents...)
	return nil
}

// CommitThenGet is a shortcut for Commit+Get, often used when resolving lock.
func (s *MvccStore) CommitThenGet(key []byte, lockTS, commitTS, getTS uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrNewEntry(key)
	err := entry.Commit(lockTS, commitTS)
	if err != nil {
		return nil, err
	}
	s.submit(entry)
	return entry.Get(getTS)
}

// Cleanup cleanups a lock, often used when resolving a expired lock.
func (s *MvccStore) Cleanup(key []byte, startTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrNewEntry(key)
	err := entry.Rollback(startTS)
	if err != nil {
		return err
	}
	s.submit(entry)
	return nil
}

// Rollback cleanups multiple locks, often used when rolling back a conflict txn.
func (s *MvccStore) Rollback(keys [][]byte, startTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ents []*mvccEntry
	for _, k := range keys {
		entry := s.getOrNewEntry(k)
		err := entry.Rollback(startTS)
		if err != nil {
			return err
		}
		ents = append(ents, entry)
	}
	s.submit(ents...)
	return nil
}

// RollbackThenGet is a shortcut for Rollback+Get, often used when resolving lock.
func (s *MvccStore) RollbackThenGet(key []byte, lockTS uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrNewEntry(key)
	err := entry.Rollback(lockTS)
	if err != nil {
		return nil, err
	}
	s.submit(entry)
	return entry.Get(lockTS)
}
