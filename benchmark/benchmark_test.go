package benchmark

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/index"
	"github.com/saint-yellow/baradb/utils"
)

var db *baradb.DB

// preparations for tests
func init() {
	opts := baradb.DefaultDBOptions
	opts.IndexType = index.Btree
	opts.Directory = "/tmp/baradb-benchmark"

	os.RemoveAll(opts.Directory)

	var err error
	db, err = baradb.LaunchDB(opts)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i <= b.N; i++ {
		err := db.Put(utils.NewKey(i), utils.NewRandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.NewKey(rand.Int()))
		if err != nil && err != baradb.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.NewKey(rand.Int()))
		if err != nil && err != baradb.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
