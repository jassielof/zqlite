# ZQLite API Documentation

**Version:** 1.3.3
**Last Updated:** 2025-10-03

## Overview

ZQLite is a PostgreSQL-compatible embedded database written in pure Zig. This document provides comprehensive API documentation for developers integrating ZQLite into their applications.

## Table of Contents

- [Core API](#core-api)
- [Connection Management](#connection-management)
- [Query Execution](#query-execution)
- [Prepared Statements](#prepared-statements)
- [Connection Pooling](#connection-pooling)
- [Query Caching](#query-caching)
- [Logging](#logging)
- [UUID Functions](#uuid-functions)
- [Error Handling](#error-handling)

---

## Core API

### Opening a Database

```zig
const std = @import("std");
const zqlite = @import("zqlite");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open a file-based database
    var conn = try zqlite.open(allocator, "mydata.db");
    defer conn.close();

    // Or use an in-memory database
    var mem_conn = try zqlite.openMemory(allocator);
    defer mem_conn.close();
}
```

### `zqlite.open(allocator, path) !*Connection`

Opens a database connection.

**Parameters:**
- `allocator: std.mem.Allocator` - Memory allocator for the connection
- `path: []const u8` - Path to database file, or `:memory:` for in-memory database

**Returns:** `*Connection` - Database connection handle

**Errors:**
- `error.FileOpenFailed` - Cannot open database file
- `error.OutOfMemory` - Memory allocation failed

### `zqlite.openMemory(allocator) !*Connection`

Opens an in-memory database connection. Equivalent to `zqlite.open(allocator, ":memory:")`.

---

## Connection Management

### `Connection.close()`

Closes the database connection and frees associated resources.

```zig
var conn = try zqlite.open(allocator, "test.db");
defer conn.close();
```

### `Connection.execute(sql: []const u8) !void`

Executes a SQL statement that doesn't return results (INSERT, UPDATE, DELETE, CREATE, etc.).

**Parameters:**
- `sql: []const u8` - SQL statement to execute

**Example:**
```zig
try conn.execute("CREATE TABLE users (id INTEGER, name TEXT)");
try conn.execute("INSERT INTO users VALUES (1, 'Alice')");
try conn.execute("UPDATE users SET name = 'Bob' WHERE id = 1");
try conn.execute("DELETE FROM users WHERE id = 1");
```

**Errors:**
- `error.ParseError` - Invalid SQL syntax
- `error.TableNotFound` - Referenced table doesn't exist
- `error.ColumnNotFound` - Referenced column doesn't exist

### `Connection.query(sql: []const u8) !QueryResult`

Executes a SQL SELECT query and returns results.

**Parameters:**
- `sql: []const u8` - SELECT query to execute

**Returns:** `QueryResult` - Query result set (must be manually freed with `result.deinit()`)

**Example:**
```zig
var result = try conn.query("SELECT * FROM users");
defer result.deinit();

while (result.next()) |row| {
    std.debug.print("Row: {any}\n", .{row});
}
```

---

## Prepared Statements

Prepared statements provide better performance and protection against SQL injection for queries that are executed multiple times.

### `Connection.prepare(sql: []const u8) !*PreparedStatement`

Creates a prepared statement.

**Parameters:**
- `sql: []const u8` - SQL query with `?` placeholders

**Example:**
```zig
var stmt = try conn.prepare("INSERT INTO users VALUES (?, ?)");
defer stmt.deinit();

try stmt.bind(1, .{ .Integer = 1 });
try stmt.bind(2, .{ .Text = "Alice" });
try stmt.execute();

try stmt.reset();
try stmt.bind(1, .{ .Integer = 2 });
try stmt.bind(2, .{ .Text = "Bob" });
try stmt.execute();
```

### `PreparedStatement.bind(index: usize, value: Value) !void`

Binds a value to a placeholder in the prepared statement.

**Parameters:**
- `index: usize` - 1-based index of the placeholder
- `value: Value` - Value to bind (see Value types below)

### `PreparedStatement.execute() !void`

Executes the prepared statement with bound values.

### `PreparedStatement.reset() !void`

Resets the prepared statement for re-execution with new bindings.

### `PreparedStatement.deinit()`

Frees the prepared statement.

---

## Connection Pooling

For high-concurrency applications, connection pooling provides significant performance benefits.

### `zqlite.createConnectionPool(allocator, database_path, min_connections, max_connections) !*ConnectionPool`

Creates a connection pool.

**Parameters:**
- `allocator: std.mem.Allocator` - Memory allocator
- `database_path: ?[]const u8` - Database file path, or `null` for in-memory
- `min_connections: u32` - Minimum number of connections to maintain
- `max_connections: u32` - Maximum number of connections allowed

**Example:**
```zig
var pool = try zqlite.createConnectionPool(allocator, "mydata.db", 4, 16);
defer pool.deinit();

var conn = try pool.acquire();
defer pool.release(conn);

try conn.execute("SELECT * FROM users");
```

### `ConnectionPool.acquire() !*Connection`

Acquires a connection from the pool (blocks if none available).

### `ConnectionPool.release(conn: *Connection)`

Returns a connection to the pool.

### `ConnectionPool.deinit()`

Closes all connections and frees the pool.

---

## Query Caching

Query caching improves performance by storing frequently-used query results.

### `zqlite.createQueryCache(allocator, max_entries, max_memory_bytes) !*QueryCache`

Creates a query cache.

**Parameters:**
- `allocator: std.mem.Allocator` - Memory allocator
- `max_entries: usize` - Maximum number of cached queries
- `max_memory_bytes: usize` - Maximum memory for cache

**Example:**
```zig
var cache = try zqlite.createQueryCache(allocator, 100, 10 * 1024 * 1024); // 100 entries, 10MB
defer cache.deinit();

var conn = try zqlite.open(allocator, "test.db");
defer conn.close();

// Cache-aware query
var result = try cache.query(conn, "SELECT * FROM users");
defer result.deinit();
```

---

## Logging

ZQLite includes a production-grade structured logging system.

### `zqlite.logging.initGlobalLogger(allocator, config)`

Initializes the global logger.

**Example:**
```zig
const logger_config = zqlite.logging.LoggerConfig{
    .level = .info,
    .format = .text,  // or .json
    .enable_colors = true,
    .enable_timestamps = true,
};

zqlite.logging.initGlobalLogger(allocator, logger_config);

zqlite.logging.info("Database initialized", .{});
zqlite.logging.warn("Connection pool at capacity", .{});
zqlite.logging.err("Query failed: {s}", .{err_msg});
```

### Log Levels

- `.debug` - Detailed diagnostic information
- `.info` - General informational messages
- `.warn` - Warning messages (potential issues)
- `.err` - Error messages
- `.fatal` - Fatal errors (application-terminating)

### Scoped Loggers

```zig
var logger = zqlite.logging.getGlobalLogger();
var scoped = zqlite.logging.ScopedLogger.init(logger, "Database");

scoped.info("Connection opened", .{});  // Outputs: [Database] Connection opened
```

---

## UUID Functions

ZQLite provides PostgreSQL-compatible UUID support.

### `zqlite.generateUUID(random: std.Random) [16]u8`

Generates a UUID v4.

**Example:**
```zig
var prng = std.rand.DefaultPrng.init(blk: {
    var seed: u64 = undefined;
    try std.os.getrandom(std.mem.asBytes(&seed));
    break :blk seed;
});
const random = prng.random();

const uuid = zqlite.generateUUID(random);
```

### `zqlite.parseUUID(uuid_str: []const u8) ![16]u8`

Parses a UUID from string format.

**Example:**
```zig
const uuid = try zqlite.parseUUID("550e8400-e29b-41d4-a716-446655440000");
```

### `zqlite.uuidToString(uuid: [16]u8, allocator: std.mem.Allocator) ![]u8`

Converts a UUID to string format.

**Example:**
```zig
const uuid_str = try zqlite.uuidToString(uuid, allocator);
defer allocator.free(uuid_str);
```

---

## Error Handling

ZQLite uses Zig's error union system for comprehensive error handling.

### Common Errors

```zig
pub const DatabaseError = error{
    // Connection errors
    FileOpenFailed,
    ConnectionClosed,
    PoolExhausted,

    // SQL errors
    ParseError,
    SyntaxError,
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
    ConstraintViolation,

    // Execution errors
    ExecutionFailed,
    TransactionFailed,
    DeadlockDetected,

    // Resource errors
    OutOfMemory,
    DiskFull,
    PermissionDenied,
};
```

### Error Handling Example

```zig
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)") catch |err| {
    switch (err) {
        error.ParseError => std.debug.print("SQL syntax error\n", .{}),
        error.OutOfMemory => std.debug.print("Out of memory\n", .{}),
        else => std.debug.print("Unexpected error: {}\n", .{err}),
    }
    return err;
};
```

---

## Value Types

ZQLite supports the following value types:

```zig
pub const Value = union(enum) {
    Integer: i64,
    Real: f64,
    Text: []const u8,
    Blob: []const u8,
    Null,
    Boolean: bool,
    UUID: [16]u8,
    Json: []const u8,
    Array: []Value,
};
```

---

## Performance Considerations

### Best Practices

1. **Use Transactions for Bulk Operations**
   ```zig
   try conn.execute("BEGIN TRANSACTION");
   for (items) |item| {
       try conn.execute("INSERT INTO items VALUES (...)");
   }
   try conn.execute("COMMIT");
   ```

2. **Use Connection Pooling for Concurrent Access**
   - Reduces connection overhead
   - Prevents connection exhaustion
   - Improves throughput

3. **Use Prepared Statements for Repeated Queries**
   - Faster execution
   - Protection against SQL injection
   - Reduced parsing overhead

4. **Use Query Caching for Read-Heavy Workloads**
   - Significantly reduces database load
   - Improves response times
   - Configurable cache size and TTL

5. **Close Resources Explicitly**
   - Use `defer` to ensure cleanup
   - Close query results after use
   - Release pooled connections promptly

---

## Examples

### Full Application Example

```zig
const std = @import("std");
const zqlite = @import("zqlite");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize logging
    const logger_config = zqlite.logging.LoggerConfig{
        .level = .info,
        .format = .text,
        .enable_colors = true,
        .enable_timestamps = true,
    };
    zqlite.logging.initGlobalLogger(allocator, logger_config);

    // Create connection pool
    var pool = try zqlite.createConnectionPool(allocator, "myapp.db", 4, 16);
    defer pool.deinit();

    zqlite.logging.info("Connection pool created", .{});

    // Acquire connection
    var conn = try pool.acquire();
    defer pool.release(conn);

    // Create schema
    try conn.execute(
        \\CREATE TABLE IF NOT EXISTS users (
        \\  id INTEGER PRIMARY KEY,
        \\  name TEXT NOT NULL,
        \\  email TEXT UNIQUE,
        \\  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        \\)
    );

    // Insert data using prepared statement
    var stmt = try conn.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
    defer stmt.deinit();

    const users = [_]struct { id: i64, name: []const u8, email: []const u8 }{
        .{ .id = 1, .name = "Alice", .email = "alice@example.com" },
        .{ .id = 2, .name = "Bob", .email = "bob@example.com" },
        .{ .id = 3, .name = "Charlie", .email = "charlie@example.com" },
    };

    for (users) |user| {
        try stmt.bind(1, .{ .Integer = user.id });
        try stmt.bind(2, .{ .Text = user.name });
        try stmt.bind(3, .{ .Text = user.email });
        try stmt.execute();
        try stmt.reset();
    }

    zqlite.logging.info("Inserted {} users", .{users.len});

    // Query data
    var result = try conn.query("SELECT id, name, email FROM users ORDER BY id");
    defer result.deinit();

    while (result.next()) |row| {
        std.debug.print("User: {any}\n", .{row});
    }

    zqlite.logging.info("Application completed successfully", .{});
}
```

---

## Version History

### v1.3.3 (Current)
- Fixed memory leaks in DEFAULT constraint handling
- Fixed double-free errors in VM execution
- Added comprehensive memory leak detection
- Added SQL parser fuzzing infrastructure
- Added structured logging system
- Added performance benchmarking suite
- Added CI benchmark regression detection

### v1.3.0
- PostgreSQL compatibility features (UUID, JSON, Arrays, Window Functions)
- Connection pooling
- Query caching
- Enhanced error handling

### v1.2.0
- Prepared statements
- Transaction support
- Basic SQL operations (CRUD)

---

## License

See LICENSE file in the repository root.

## Contributing

See CONTRIBUTING.md for development guidelines.

## Support

For issues and questions, please visit: https://github.com/yourusername/zqlite/issues
