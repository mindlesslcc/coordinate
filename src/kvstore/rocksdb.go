package kvstore

import (
	"errors"
	"sync"

	"github.com/tecbot/gorocksdb"
)

type RocksDB struct {
	rdb *gorocksdb.DB
	mu  sync.Mutex
}

func initRocksDB() {
}

// GetLeverdb from dictionary
func GetRocksDB(path string) (DB, error) {
	var err error
	var rdb RocksDB

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	rdb.rdb, err = gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}
	return rdb, nil
}

func (rdb RocksDB) Release() {
	rdb.rdb.Close()
}

func (rdb RocksDB) Insert(key, value []byte) error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	wo := gorocksdb.NewDefaultWriteOptions()
	return rdb.rdb.Put(wo, key, value)
}

func (rdb RocksDB) Delete(key []byte) error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	wo := gorocksdb.NewDefaultWriteOptions()
	return rdb.rdb.Delete(wo, key)
}

func (rdb RocksDB) Exist(key []byte) (bool, error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	ro := gorocksdb.NewDefaultReadOptions()
	value, err := rdb.rdb.Get(ro, key)
	return value.Data() != nil, err
}

func (rdb RocksDB) Lookup(key []byte) ([]byte, error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	ro := gorocksdb.NewDefaultReadOptions()
	slice, err := rdb.rdb.Get(ro, key)
	return slice.Data(), err
}

func (rdb RocksDB) GetAll() (data map[string][]byte, err error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	defer func() {
		if p := recover(); p != nil {
			str, ok := p.(string)
			if ok {
				err = errors.New(str)
			} else {
				err = errors.New("panic in rocksdb get all")
			}
		}
	}()
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := rdb.rdb.NewIterator(ro)
	defer it.Close()
	data = make(map[string][]byte)
	it.Seek([]byte(""))

	// get all with no prefix and no options
	for it = it; it.Valid(); it.Next() {
		v := string(it.Value().Data())
		data[string(it.Key().Data())] = []byte(v)
	}
	return data, nil
}

func (rdb RocksDB) LookupWithPrefix(prefix []byte) (map[string][]byte, error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := rdb.rdb.NewIterator(ro)
	defer it.Close()
	data := make(map[string][]byte)
	it.Seek(prefix)

	for it = it; it.Valid(); it.Next() {
		data[string(it.Key().Data())] = it.Value().Data()
	}
	return data, nil

}

func (rdb RocksDB) DeleteWithPrefix(prefix []byte) error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := rdb.rdb.NewIterator(ro)
	it.Seek(prefix)
	defer it.Close()

	for it = it; it.Valid(); it.Next() {
		err := rdb.rdb.Delete(gorocksdb.NewDefaultWriteOptions(), it.Key().Data())
		if err != nil {
			return err
		}
	}
	return nil
}

// cleanup will delete all db content
func (rdb RocksDB) CleanUp() error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := rdb.rdb.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte(""))

	for it = it; it.Valid(); it.Next() {
		err := rdb.rdb.Delete(gorocksdb.NewDefaultWriteOptions(), it.Key().Data())
		if err != nil {
			return err
		}
	}
	return nil
}

// test is empty
func (rdb RocksDB) IsEmpty() (bool, error) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	ro := gorocksdb.NewDefaultReadOptions()
	it := rdb.rdb.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte(""))

	for it = it; it.Valid(); it.Next() {
		return false, nil
	}
	return true, nil
}

func (rdb RocksDB) GetSnapshot() ([]byte, error) {
	return []byte(""), nil
}

func (rdb RocksDB) RecoverFromSnapshot([]byte) error {
	return nil
}

/* debug function,just reset leverdb to initial state */
func (rdb RocksDB) ResetDB() {
	//cleanup db
}
