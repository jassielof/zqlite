# Changelog

All notable changes to ZQLite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.5] - 2025-12-01

### Changed
- Pinned CI to Zig `0.16.0-dev.1484+d0ba6642b` for reproducible builds
- Reorganized project structure: moved test files to `tests/standalone/`
- Cleaned up root directory (removed build artifacts, temp files)
- Updated `.gitignore` to prevent future cruft accumulation

### Fixed
- Version mismatch between `build.zig.zon` and `src/version.zig`

## [1.3.4] - 2025-10-03

### Fixed
- **Critical B-tree OrderMismatch bug** - Now supports large datasets
  - Fixed missing `writeNode()` after `splitChild()`
  - Fixed array bounds checking in child index calculation
  - Validated with 5,000 row insertions (2,064 ops/sec)

## [1.3.3] - 2025-10-03

### Fixed
- Memory leaks in `planner.zig` (DEFAULT constraint handling)
- Double-free errors in `vm.zig` (`cloneStorageDefaultValue`)

### Added
- Memory leak detection in CI using `GeneralPurposeAllocator`
- Fuzzing infrastructure for SQL parser
- Fuzzing infrastructure for VM execution
- Structured logging system
- Comprehensive benchmarking suite
- Benchmark regression detection in CI

## [1.0.0] - 2025-09-01

### Added
- **Core SQL Engine**
  - Full CRUD operations: CREATE, INSERT, SELECT, UPDATE, DELETE
  - B-tree storage engine with Write-Ahead Logging (WAL)
  - SQL parser with AST generation
  - Query planner and VM executor
  - Prepared statements support

- **Advanced Indexing**
  - B-tree indexes for range queries
  - Hash indexes for O(1) lookups
  - Unique constraint indexes
  - Multi-column composite indexes

- **Post-Quantum Cryptography** (optional)
  - ML-KEM-768 key encapsulation
  - ML-DSA-65 digital signatures
  - Hybrid classical + post-quantum modes
  - Field-level encryption (ChaCha20-Poly1305)

- **Concurrency**
  - MVCC transaction isolation
  - Connection pooling
  - Thread-safe operations
  - Async database operations

- **Additional Features**
  - JSON data type support
  - C API / FFI for language bindings
  - In-memory and file-based databases
  - CLI shell interface

## [0.3.0] - 2025-06-23

### Added
- Advanced indexing system (B-tree, hash, composite)
- Cryptographic engine with AES-256-GCM, ChaCha20-Poly1305
- Async operations with connection pooling
- JSON support
- C API for FFI

### Changed
- Updated to Zig 0.15.0-dev compatibility

## [0.2.0] - 2025-05-01

### Added
- Basic SQL operations
- B-tree storage engine
- WAL support
- DNS/PowerDNS integration examples
- CLI interface

## [0.1.0] - 2025-03-01

### Added
- Initial release
- Basic embedded SQL database
- Core storage engine
- Simple query parser
