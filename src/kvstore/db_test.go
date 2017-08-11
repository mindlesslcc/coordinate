package kvstore

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
)

const (
	TEST_COUNT = 10
)

var i int
var err error
var data []byte

func testDB(dbType DBType, t *testing.T) {
	var exist bool
	//test get/put db
	db, err := GetDB(dbType, "tmp")
	if db == nil || err != nil {
		t.FailNow()
	}
	defer os.RemoveAll("tmp")
	defer db.Release()
	//test clean up
	db.CleanUp()
	var isEmpty bool
	if isEmpty, err = db.IsEmpty(); !isEmpty || err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println("DB Test: test clean up db OK")
	//test insert
	for i = 0; i < TEST_COUNT; i++ {
		err = db.Insert([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
		if err != nil {
			fmt.Print(err)
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test insert db OK")
	// test get snapshot
	var snapshot []byte
	snapshot, err = db.GetSnapshot()
	if err != nil {
		fmt.Println("get snapshot error")
		t.FailNow()
	}
	fmt.Println("DB Test: test get snap db OK")
	// test recover from snapshot
	err = db.RecoverFromSnapshot(snapshot)
	if err != nil {
		fmt.Println("recover snapshot error")
		t.FailNow()
	}
	for i := 0; i < 10; i++ {
		exist, err = db.Exist([]byte(fmt.Sprintf("key%d", i)))
		if err != nil || exist == false {
			fmt.Println("after recover not exist")
			t.FailNow()
		}
	}
	for i := 10; i < 20; i++ {
		exist, err = db.Exist([]byte(fmt.Sprintf("key%d", i)))
		if err != nil || exist == true {
			fmt.Println("after recover error exist")
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test recover db OK")
	//test getall
	all, err := db.GetAll()
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	for i = 0; i < TEST_COUNT; i++ {
		if false == reflect.DeepEqual(all["key"+strconv.Itoa(i)], []byte("value"+strconv.Itoa(i))) {
			fmt.Println(all["key"+strconv.Itoa(i)])
			fmt.Println("value" + strconv.Itoa(i))
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test getall db OK")
	// test delete with prefix
	err = db.DeleteWithPrefix([]byte("key"))
	if err != nil {
		fmt.Println("err delete with prefix key")
		t.FailNow()
	}
	for i := 0; i < 10; i++ {
		exist, err = db.Exist([]byte(fmt.Sprintf("key%d", i)))
		if exist == true {
			fmt.Println("deleted key exist!")
			t.FailNow()
		}
	}
	for i = 0; i < TEST_COUNT; i++ {
		err = db.Insert([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
		if err != nil {
			fmt.Print(err)
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test delete with prefix db OK")
	//test exist
	for i := 0; i < 10; i++ {
		exist, err = db.Exist([]byte(fmt.Sprintf("key%d", i)))
		if err != nil || exist == false {
			t.FailNow()
		}
	}
	for i := 10; i < 20; i++ {
		exist, err = db.Exist([]byte(fmt.Sprintf("key%d", i)))
		if err != nil || exist == true {
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test exist db OK")
	//test lookup
	for i = 0; i < 10; i++ {
		data, err = db.Lookup([]byte("key" + strconv.Itoa(i)))
		if err != nil {
			t.FailNow()
		}
		if string(data) != "value"+strconv.Itoa(i) {
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test lookup db OK")
	//test delete
	for i = 0; i < 10; i++ {
		err = db.Delete([]byte("key" + strconv.Itoa(i)))
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
	}
	for i = 0; i < 10; i++ {
		data, err = db.Lookup([]byte("key" + strconv.Itoa(i)))
		if data != nil && err == nil {
			fmt.Println(data)
			t.FailNow()
		}
	}
	fmt.Println("DB Test: test delete db OK")
	if err = os.RemoveAll("tmp"); err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestMapDB(t *testing.T) {
	testDB(MAP_DB, t)
}

func TestLevelDB(t *testing.T) {
	testDB(LEVEL_DB, t)
}

func TestRocksDB(t *testing.T) {
	testDB(ROCKS_DB, t)
}

func TestGetPutDB(t *testing.T) {
	var db DB
	var err error
	for i := 0; i < 10000; i++ {
		if db, err = GetDB(MAP_DB, ""); err != nil || db == nil {
			fmt.Println("get map db's err should not be nil!")
			t.FailNow()
		}
		defer db.Release()
	}

	for i := 0; i < 100; i++ {
		if db, err = GetDB(LEVEL_DB, fmt.Sprintf("%d", i)); err != nil || db == nil {
			fmt.Printf("get level db's err should not be nil : %s\n", err)
			t.FailNow()
		}
		defer db.Release()
	}
	for i := 0; i < 100; i++ {
		if err = os.RemoveAll(strconv.Itoa(i)); err != nil {
			fmt.Println(err)
			t.FailNow()
		}
	}
	for i := DB_NUM; i < 10000; i++ {
		if db, err = GetDB(DBType(i), fmt.Sprintf("%d", i)); err == nil || db != nil {
			fmt.Printf("should get invalid args %s\n", err)
			t.FailNow()
		}
	}
}
