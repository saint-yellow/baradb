package main

import (
	"fmt"

	"github.com/saint-yellow/baradb"
)

func main() {
	opts := baradb.DefaultDBOptions
	db, err := baradb.LaunchDB(opts)
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
}
