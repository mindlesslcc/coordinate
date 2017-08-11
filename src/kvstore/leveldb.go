package kvstore

import (
	"encoding/json"
	_ "errors"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	ldb      *leveldb.DB
	refCount uint32
	mu       sync.Mutex
}

// log all leveldbs
var levelDBs map[string]LevelDB

func initLevelDB() {
}

// GetLeveldb from dictionary
func GetLevelDB(path string) (DB, error) {
	var err error
	if v, ok := levelDBs[path]; ok {
		return v, nil
	}
	var ldb LevelDB
	ldb.ldb, err = leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return ldb, nil
}

func (ldb LevelDB) Release() {
	ldb.ldb.Close()
}

func (ldb LevelDB) Insert(key, value []byte) error {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	return ldb.ldb.Put(key, value, nil)
}

func (ldb LevelDB) Delete(key []byte) error {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	return ldb.ldb.Delete(key, nil)
}

func (ldb LevelDB) Exist(key []byte) (bool, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	return ldb.ldb.Has(key, nil)
}

func (ldb LevelDB) Lookup(key []byte) ([]byte, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	return ldb.ldb.Get(key, nil)
}

func (ldb LevelDB) GetAll() (map[string][]byte, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	data := make(map[string][]byte)

	// get all with no prefix and no options
	iter := ldb.ldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		v := string(iter.Value())
		data[string(iter.Key())] = []byte(v)
	}
	return data, nil
}

func (ldb LevelDB) LookupWithPrefix(prefix []byte) (map[string][]byte, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	data := make(map[string][]byte)
	iter := ldb.ldb.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()
	for iter.Next() {
		data[string(iter.Key())] = iter.Value()
	}
	return data, nil

}

func (ldb LevelDB) DeleteWithPrefix(prefix []byte) error {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	iter := ldb.ldb.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()
	for iter.Next() {
		err := ldb.ldb.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// cleanup will delete all db content
func (ldb LevelDB) CleanUp() error {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	iter := ldb.ldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		ldb.Delete(iter.Key())
	}
	return nil
}

// test is empty
func (ldb LevelDB) IsEmpty() (bool, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	iter := ldb.ldb.NewIterator(nil, nil)
	defer iter.Release()
	if iter.Next() {
		return false, nil
	}
	return true, nil
}

func (ldb LevelDB) GetSnapshot() ([]byte, error) {
	var snap *leveldb.Snapshot
	var data []byte
	var err error
	var kvs map[string][]byte
	kvs = make(map[string][]byte)

	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	snap, err = ldb.ldb.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	iter := snap.NewIterator(nil, nil)
	for iter.Next() {
		v := string(iter.Value())
		kvs[string(iter.Key())] = []byte(v)
	}

	data, _ = json.Marshal(&kvs)
	return data, nil
}

func (ldb LevelDB) RecoverFromSnapshot(snap []byte) error {
	var err error
	var kvs map[string][]byte
	kvs = make(map[string][]byte)

	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	iter := ldb.ldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		ldb.ldb.Delete(iter.Key(), nil)
	}

	err = json.Unmarshal(snap, &kvs)
	if err != nil {
		return err
	}
	for k, v := range kvs {
		ldb.ldb.Put([]byte(k), v, nil)
	}
	return nil
}

/* debug function,just reset levelDB to initial state */
func (ldb LevelDB) ResetDB() {
	//cleanup db
	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	iter := ldb.ldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		ldb.Delete(iter.Key())
	}
}
