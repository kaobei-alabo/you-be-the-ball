package lan

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

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

// snapshots keeps the latest snapshot in RAM. Unlike raft.InmemSnapshotStore it
// shows a snapshot only once it is whole: that one lists it as soon as it is
// begun, so the host could send a player half of it.
type snapshots struct {
	mu   sync.Mutex
	meta *raft.SnapshotMeta // nil until the first is complete
	data []byte
}

type sink struct {
	s    *snapshots
	meta raft.SnapshotMeta
	buf  bytes.Buffer
}

func (s *snapshots) Create(v raft.SnapshotVersion, index, term uint64, c raft.Configuration, ci uint64, _ raft.Transport) (raft.SnapshotSink, error) {
	id := fmt.Sprintf("%d-%d-%d", term, index, time.Now().UnixMilli())
	return &sink{s: s, meta: raft.SnapshotMeta{Version: v, ID: id, Index: index, Term: term, Configuration: c, ConfigurationIndex: ci}}, nil
}

func (s *snapshots) List() ([]*raft.SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.meta == nil {
		return nil, nil
	}
	m := *s.meta
	return []*raft.SnapshotMeta{&m}, nil
}

func (s *snapshots) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.meta == nil || s.meta.ID != id {
		return nil, nil, fmt.Errorf("no snapshot %s", id)
	}
	m := *s.meta
	return &m, io.NopCloser(bytes.NewReader(s.data)), nil
}

func (k *sink) Write(b []byte) (int, error) { return k.buf.Write(b) }
func (k *sink) ID() string                  { return k.meta.ID }
func (k *sink) Cancel() error               { return nil }

// Close publishes the snapshot; its bytes are never written again. One that
// finishes after a newer one is dropped: the log before the newer is gone.
func (k *sink) Close() error {
	k.meta.Size = int64(k.buf.Len())
	k.s.mu.Lock()
	defer k.s.mu.Unlock()
	if k.s.meta == nil || k.meta.Index >= k.s.meta.Index {
		k.s.meta, k.s.data = &k.meta, k.buf.Bytes()
	}
	return nil
}
