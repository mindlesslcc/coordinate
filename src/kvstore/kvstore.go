package kvstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/coreos/etcd/snap"
)

// a key-value store backed by raft
type KVstore struct {
	proposeC    chan<- string // channel for proposing updates
	db          DB
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func NewKVstore(id int, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *KVstore {
	var err error
	s := &KVstore{proposeC: proposeC, snapshotter: snapshotter}
	if s.db, err = GetDB(LEVEL_DB, fmt.Sprintf("raft%d", id)); err != nil {
		return nil
	}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into KVstore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *KVstore) Lookup(key string) (string, bool) {
	v, ok := s.db.Lookup([]byte(key))
	return string(v), ok == nil
}

func (s *KVstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- string(buf.Bytes())
}

func (s *KVstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("coordinate: could not decode message (%v)", err)
		}
		s.db.Insert([]byte(dataKv.Key), []byte(dataKv.Val))
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *KVstore) GetSnapshot() ([]byte, error) {
	return s.db.GetSnapshot()
}

func (s *KVstore) recoverFromSnapshot(snapshot []byte) error {
	s.db.RecoverFromSnapshot(snapshot)
	return nil
}
