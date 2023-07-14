# baradb

**baradb** is a K/V storage engine based on Bitcask and inspired by [rosedblabs/rosedb](https://github.com/rosedblabs/rosedb)

bara: バラ, Japanese name of rose

## Design Overview 
![design-overview](https://github.com/saint-yellow/baradb/blob/main/documentation/images/design-overview.png)

## Install

```shell 
$ go get -u github.com/saint-yellow/bradb
```

## Usage

### Basic Operations 

```go

// launch a DB engine 
opts := baradb.DefaultDBOptions
db, err := baradb.Launch(opts)
if err != nil {
    panic(err)
}

// put a key/value pair to the DB engine
err = db.Put([]byte("114514"), []byte("1919810"))
if err != nil {
    panic(err)
}

// get vlue of a key from the DB engine  
value, err := db.Get([]byte("114514"))
if err != nil {
    panic(err)
}
fmt.Println(string(value))

// delete a key/value pair from the DB engine
err = db.Delete([]byte("114514"))
if err != nil {
    panic(err)
}
```

### Batch Operations 

```go
// initialze a write batch
wb := db.NewWriteBatch(baradb.DefaultWriteBatchOptions)

// put a key/value pair to the write batch 
err = wb.Put([]byte("1919"), []byte("1919"))
if err != nil {
    panic(err)
}

// the DB engine will not store data in the write batch 
// since the write batch is not committed
value, err = db.Get([]byte("1919"))
if err != nil {
    panic(err)
}
fmt.Println(string(value))

// delete a key/value pair in the write batch 
err = wb.Delete([]byte("114514"))
if err != nil {
    panic(err)
}

// commit the write batch 
err = wb.Commit()
if err != nil {
    panic(err)
}

// the DB engine can get data in the write batch 
// that is successfully committed
value, err = db.Get([]byte("1919"))
if err != nil {
    panic(err)
}
fmt.Println(string(value))
```

## Features 

### Strengths 
- Low latency per item read or written 
- High throughput, especially when writing an incoming stream of random items 
- Ability to handle datasets larger than RAM without degradation 
- Single seek to retrieve any value 
- Predictable lookup and insert performance 
- Easy backup 
- Batch options which guarantee atomicity, consistency, and durability 
- Support iterator for forward and backward 

### Weaknesses
- Keys must fit in memory
- Startup speed is affected by the amount of data

## Applications 

Integrate HTTP into the K/V storage engine 

- Case: [baradb-http](https://github.com/saint-yellow/baradb-http)

Implement Redis on the top of the K/V storage engine

- Case: [baradb-redis](https://github.com/saint-yellow/baradb-redis)

And... More!

## Contributing

PRs accepted.

## License

MIT © Saint-Yellow 
