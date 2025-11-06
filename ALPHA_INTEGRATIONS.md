# Alpha Integration Status & Roadmap

This document tracks the maturity status of all Zig libraries used in Wraith and provides a roadmap for stabilizing alpha/experimental projects to production-ready (RC/1.0) quality.

**Target**: Get all dependencies to **Release Candidate (RC)** or **1.0** status before Wraith reaches production.

---

## 🟢 Production Ready (RC/1.0)

These libraries are stable and ready for production use:

### zsync - Async Runtime
- **Status**: ✅ RC Quality
- **Maintainer**: ghostkellz
- **Wraith Use**: Core async runtime, event loop foundation
- **Notes**: Battle-tested, high-performance, ready to use

### zpack - Compression Library
- **Status**: ✅ RC Quality
- **Maintainer**: ghostkellz
- **Wraith Use**: Gzip, Brotli compression for HTTP responses
- **Notes**: Fast, reliable compression algorithms

### gcode - Unicode Processing
- **Status**: ✅ RC Quality
- **Maintainer**: ghostkellz
- **Wraith Use**: Terminal UI rendering, text processing
- **Notes**: Stable for terminal applications

---

## 🟡 Alpha/Beta - Needs Stabilization

These libraries are functional but need work to reach production quality:

---
### zqlite - Embedded SQL Database
- **Status**: ⚠️ Alpha/Beta (needs verification)
- **Repository**: https://github.com/ghostkellz/zqlite
- **Wraith Use**: **CRITICAL** - Queryable logs, metrics, alerts, persistent state (unique feature!)

#### What Wraith Needs:
1. **SQL Query Engine** - SELECT, INSERT, UPDATE, DELETE
2. **Indexes** - B-tree indexes for fast queries
3. **Transactions** - ACID compliance for writes
4. **Concurrent Access** - Multiple readers, single writer (or better)
5. **Time-Series Optimization** - Efficient for access logs (append-heavy)
6. **WAL Mode** - Write-Ahead Logging for durability
7. **Schema Management** - DDL support (CREATE TABLE, ALTER, etc.)
8. **Backup/Export** - SQL dump, CSV export
9. **Query Optimization** - Query planner, execution optimizer
10. **Connection Pooling** - Efficient multi-threaded access

#### Stabilization Checklist:
- [ ] SQL compliance testing (SQLite test suite?)
- [ ] Concurrent access stress testing
- [ ] ACID property verification
- [ ] Performance benchmarking (vs. SQLite, DuckDB)
- [ ] Time-series workload optimization (writes, range queries)
- [ ] Memory efficiency under large datasets
- [ ] Crash recovery testing
- [ ] Corruption detection and repair
- [ ] Query planner optimization
- [ ] Index performance tuning
- [ ] Documentation (SQL reference, API docs)
- [ ] Integration examples with zsync (async queries)

#### Current Gaps (Estimated):
- May lack advanced SQL features (JOINs, subqueries, CTEs)
- Time-series optimization may need work
- Concurrent access under load needs testing
- Query planner may be naive

#### Wraith-Specific Features Needed:
- Pre-built schemas for access logs, metrics, alerts
- Optimized indexes for common queries (by IP, by time range, by status code)
- Automatic log rotation/archival (move old data to compressed archives)
- Real-time materialized views (top IPs, error rates, latency percentiles)

