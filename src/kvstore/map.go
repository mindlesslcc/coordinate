package kvstore

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

// MapDB -
type MapDB struct {
	db       map[string][]byte
	refCount uint32
	mu       sync.Mutex
}

func initMapDB() {
}

// GetMapDB haha
func GetMapDB() (DB, error) {
	var mdb MapDB
	mdb.db = make(map[string][]byte)
	return mdb, nil
}

// Release db
func (mdb MapDB) Release() {
}

// Insert a k,v
func (mdb MapDB) Insert(key, value []byte) error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	mdb.db[string(key)] = value
	return nil
}

// Delete a Key
func (mdb MapDB) Delete(key []byte) error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	delete(mdb.db, string(key))
	return nil
}

// Exist a key or not
func (mdb MapDB) Exist(key []byte) (bool, error) {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	_, exist := mdb.db[string(key)]
	return exist, nil
}

// Lookup for a k,v
func (mdb MapDB) Lookup(key []byte) ([]byte, error) {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	val, exist := mdb.db[string(key)]
	if !exist {
		return nil, errors.New("do not exist")
	}
	return val, nil
}

// LookupWithPrefix lookup prefix
func (mdb MapDB) LookupWithPrefix(prefix []byte) (map[string][]byte, error) {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	data := make(map[string][]byte)
	for k, v := range mdb.db {
		if strings.HasPrefix(k, string(prefix)) {
			data[string(k)] = v
		}
	}
	return data, nil
}

// LookupWithPrefix lookup prefix
func (mdb MapDB) DeleteWithPrefix(prefix []byte) error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	for k, _ := range mdb.db {
		if strings.HasPrefix(k, string(prefix)) {
			delete(mdb.db, string(k))
		}
	}
	return nil
}

// GetAll get all data
func (mdb MapDB) GetAll() (map[string][]byte, error) {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	data := make(map[string][]byte)
	for k, v := range mdb.db {
		data[string(k)] = v
	}
	return data, nil
}

// CleanUp will delete all db content
func (mdb MapDB) CleanUp() error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()
	for k := range mdb.db {
		delete(mdb.db, k)
	}
	return nil
}

// IsEmpty is empty or not
func (mdb MapDB) IsEmpty() (bool, error) {
	return len(mdb.db) == 0, nil
}

func (mdb MapDB) GetSnapshot() ([]byte, error) {
	data, err := json.Marshal(mdb.db)
	if err != nil {
		return []byte(""), err
	}
	return data, nil
}

func (mdb MapDB) RecoverFromSnapshot(bytes []byte) error {
	err := json.Unmarshal(bytes, &mdb.db)
	if err != nil {
		return err
	}
	return nil
}

// ResetDB is no
func (mdb MapDB) ResetDB() {
}
