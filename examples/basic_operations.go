package main

import (
	"fmt"

	"github.com/saint-yellow/baradb"
)

func main() {
	opts := baradb.DefaultDBOptions
	db, err := baradb.Launch(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("114514"), []byte("1919810"))
	if err != nil {
		panic(err)
	}

	value, err := db.Get([]byte("114514"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))

	err = db.Delete([]byte("114514"))
	if err != nil {
		panic(err)
	}

	wb := db.NewWriteBatch(baradb.DefaultWriteBatchOptions)

	err = wb.Put([]byte("1919"), []byte("1919"))
	if err != nil {
		panic(err)
	}

	value, err = db.Get([]byte("1919"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))

	err = wb.Delete([]byte("114514"))
	if err != nil {
		panic(err)
	}

	err = wb.Commit()
	if err != nil {
		panic(err)
	}

	value, err = db.Get([]byte("1919"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))
}
