package lan

import (
	"bytes"
	"errors"
	"sync"

	"github.com/hashicorp/raft"
)

// store keeps raft's log and votes in RAM. Unlike raft.InmemStore it copies on
// the way in and out, so raft never shares a buffer with a caller.
type store struct {
	mu     sync.Mutex
	logs   map[uint64]raft.Log
	lo, hi uint64
	peak   int // the most entries held at once, for tests
	kv     map[string][]byte
	ints   map[string]uint64
}

func newStore() *store {
	return &store{logs: make(map[uint64]raft.Log), kv: make(map[string][]byte), ints: make(map[string]uint64)}
}

func copyLog(l raft.Log) raft.Log {
	l.Data = bytes.Clone(l.Data)
	l.Extensions = bytes.Clone(l.Extensions)
	return l
}

func (s *store) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lo, nil
}

func (s *store) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hi, nil
}

func (s *store) GetLog(i uint64, out *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.logs[i]
	if !ok {
		return raft.ErrLogNotFound
	}
	*out = copyLog(l)
	return nil
}

func (s *store) StoreLog(l *raft.Log) error { return s.StoreLogs([]*raft.Log{l}) }

func (s *store) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range logs {
		s.logs[l.Index] = copyLog(*l)
		if s.lo == 0 || l.Index < s.lo {
			s.lo = l.Index
		}
		s.hi = max(s.hi, l.Index)
	}
	s.peak = max(s.peak, len(s.logs))
	return nil
}

// DeleteRange cuts a prefix (compaction) or a suffix (a conflicting tail).
func (s *store) DeleteRange(lo, hi uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lo, hi = max(lo, s.lo), min(hi, s.hi)
	if s.hi == 0 || lo > hi {
		return nil
	}
	for i := lo; i <= hi; i++ {
		delete(s.logs, i)
	}
	if lo == s.lo {
		s.lo = hi + 1
	}
	if hi == s.hi {
		s.hi = lo - 1
	}
	if s.lo > s.hi {
		s.lo, s.hi = 0, 0
	}
	// A Go map keeps its peak size after deletes: copy what is left, so a
	// compacted log frees its memory.
	if hi-lo+1 > uint64(len(s.logs)) {
		m := make(map[uint64]raft.Log, len(s.logs))
		for i, l := range s.logs {
			m[i] = l
		}
		s.logs = m
	}
	return nil
}

// IsMonotonic: after installing a snapshot raft drops the whole old log rather
// than keep a tail with a gap before the entries that follow the snapshot.
func (*store) IsMonotonic() bool { return true }

func (s *store) Set(k, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[string(k)] = bytes.Clone(v)
	return nil
}

// Get must fail with exactly "not found" for a missing key: raft matches the text.
func (s *store) Get(k []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[string(k)]
	if !ok {
		return nil, errors.New("not found")
	}
	return bytes.Clone(v), nil
}

func (s *store) SetUint64(k []byte, v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ints[string(k)] = v
	return nil
}

func (s *store) GetUint64(k []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ints[string(k)], nil
}
