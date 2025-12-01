# ZQLite Roadmap

**Current Version:** 1.3.5
**Status:** Production Ready

## Completed

### v1.3.5 (2025-12-01)
- Pinned CI to specific Zig version for reproducible builds
- Cleaned up project structure
- Updated documentation

### v1.3.4 (2025-10-03)
- Fixed critical B-tree OrderMismatch bug
- Now supports large datasets (5,000+ rows tested at 2,064 ops/sec)

### v1.3.3 (2025-10-03)
- Fixed memory leaks in DEFAULT constraint handling
- Fixed double-free errors in VM execution
- Added memory leak detection to CI
- Added SQL parser fuzzing
- Added structured logging
- Added benchmark suite with CI regression detection

## Future

### Performance
- [ ] Optimize hot paths identified in benchmarks
- [ ] Connection pool stress testing with 10,000+ rows
- [ ] Query plan caching

### Testing
- [ ] Expand fuzzing to VM execution paths
- [ ] Stress tests with 50,000+ rows
- [ ] Concurrent access stress tests

### Features
- [ ] Transaction savepoints
- [ ] Full-text search
- [ ] Database backup/restore
- [ ] Query EXPLAIN

### Documentation
- [ ] Architecture diagrams
- [ ] Performance tuning guide
- [ ] Migration guide from SQLite
