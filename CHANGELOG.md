# CHANGELOG

## v0.1.1 (2023-07-12)

### Others

- docs: format code (2023-07-12)

- docs: explain usage (2023-07-12)

- docs: update README.md (2023-07-12)

- style: format (2023-07-12)

- test(db): add unit test for forking DB engine (2023-07-10)

- refactor(db): rename function LaunchDB to Launch (2023-07-07)

- refactor(index): rename module indexer to index (2023-07-06)

- test(db): update unit tests (2023-07-05)

- refactor(indexer): make some concrete structs invisible out their package (2023-07-05)

- refactor(io_handler): make concrete I/O handlers invisible outside their package (2023-07-05)

- refactor(indexer): make concrete indexers invisible outside their package (2023-07-05)

- docs: update README.md (2023-07-03)

- refactor(redis): remove redis module (2023-07-02)

## v0.1.0 (2023-07-12)

### Added

- feat(redis): design Redis data structure: zset (2023-07-01)

- feat(redis): design Redis data structure: list (2023-06-30)

- feat(redis): design Redis data structure: set (2023-06-30)

- feat(redis): design Redis data structure: hash (2023-06-29)

- feat(redis): design data structures: string (2023-06-29)

- feat(db): add support of HTTP service (2023-06-28)

- feat(db): add support of data backup (2023-06-27)

- feat(db): design the process of deleting data (2023-06-08)

- feat(db): design the proccess of launching DB engine instance (2023-06-08)

- feat(db): design the processes of data reading and writing (2023-06-08)

- feat(indexer): design a data structure of a log record (2023-06-06)

- feat(indexer): design indexer (2023-06-06)

### Fixed

- fix(db): fix a bug of appending a log record (2023-06-13)

### Others

- docs: format code (2023-07-12)

- docs: explain usage (2023-07-12)

- docs: update README.md (2023-07-12)

- style: format (2023-07-12)

- test(db): add unit test for forking DB engine (2023-07-10)

- refactor(db): rename function LaunchDB to Launch (2023-07-07)

- refactor(index): rename module indexer to index (2023-07-06)

- test(db): update unit tests (2023-07-05)

- refactor(indexer): make some concrete structs invisible out their package (2023-07-05)

- refactor(io_handler): make concrete I/O handlers invisible outside their package (2023-07-05)

- refactor(indexer): make concrete indexers invisible outside their package (2023-07-05)

- docs: update README.md (2023-07-03)

- refactor(redis): remove redis module (2023-07-02)

- test(redis): update unit test for Redis command TYPE (2023-07-01)

- refactor(redis): rename some methods for encoding an internal key to a byte array (2023-06-30)

- style(db): move some functions to other files (2023-06-30)

- style(redis): add comments (2023-06-29)

- refactor(redis): clear unsed dependencies since the http module was removed (2023-06-29)

- refactor(http): remove the http module (2023-06-29)

- test: add benchmarks (2023-06-28)

- refactor(db, log_record): move and rename methods used to encode/decode a key (2023-06-28)

- Update README.md (2023-06-27)

- docs: update README.md (2023-06-27)

- refactor(data,utils,db): follow suggestions provided by LSP diagnosis (2023-06-27)

- style(data, db): uncapitalize the messages of all user-defined errors (2023-06-27)

- perf(db, indexer): optimize the mergence of log records (2023-06-27)

- perf(db,io_handler,indexer): support file lock and memory map (2023-06-26)

- feat(indexer, db): optimize in-memory indexer (2023-06-24)

- feat(db, data): design the process of merging data (2023-06-21)

- feat(db, batch, data): design batch writing (2023-06-19)

- test(db): close the entire DB instad of an active data file (2023-06-15)

- refactor(db): check errors when loading index (2023-06-15)

- refactor(data_file): rename DataFileSuffix -> DataFileNameSuffix (2023-06-15)

- refactor(db): rename: Options -> DBOptions (2023-06-15)

- feat(indexer.iterator, db.iterator): design iterator (2023-06-14)

- refactor(indexer): make some fields public (2023-06-14)

- test(db, data_file, indexer): add tests (2023-06-13)

- refactor(data_file): refactor the method of writing a log record to a data file (2023-06-13)

- fix(data_file): fix a bug that occurred when writing a log record to a data file (2023-06-13)

- test(db): add a teardown method to clear data files (2023-06-13)

- test(db): add example of basic operations of DB engine (2023-06-13)

- test(db): add tests for DB engine (2023-06-13)

- test(data_file): add test (2023-06-12)

- refactor(io_handler, indexer): rename modules (2023-06-12)

- test(log_record): implement and test the process of encoding/decoding a log record (2023-06-10)

- test(db_file): test the data file module (2023-06-08)

- fix(io_handler): modify the permission of every single data file (2023-06-08)

- feat(data_file, io_handler): design the data file module (2023-06-08)

- feat(i/o): design I/O (2023-06-06)

- Initialize project (2023-06-06)

- Initial commit (2023-06-06)
