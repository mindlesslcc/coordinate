package kvstore

import (
	"errors"
)

type DBType int

const (
	MAP_DB DBType = iota
	LEVEL_DB
	ROCKS_DB
	DB_NUM
)

type DB interface {
	Release()
	Insert(key, value []byte) error
	Delete(key []byte) error
	Exist(key []byte) (bool, error)
	Lookup(key []byte) ([]byte, error)
	GetAll() (map[string][]byte, error)
	LookupWithPrefix(prefix []byte) (map[string][]byte, error)
	DeleteWithPrefix(prefix []byte) error
	IsEmpty() (bool, error)
	CleanUp() error

	GetSnapshot() ([]byte, error)
	RecoverFromSnapshot([]byte) error

	// debug function
	ResetDB()
}

func init() {
	initMapDB()
	initLevelDB()
	initRocksDB()
}

func GetDB(db DBType, dbPath string) (DB, error) {
	if db == MAP_DB {
		return GetMapDB()
	} else if db == LEVEL_DB {
		return GetLevelDB(dbPath)
	} else if db == ROCKS_DB {
		return GetRocksDB(dbPath)
	} else {
		return nil, errors.New("invalid args")
	}
}
