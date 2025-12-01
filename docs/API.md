# ZQLite API Reference

**Version:** 1.3.5

## Core API

### Database Connection

```zig
const zqlite = @import("zqlite");

// File-based database
var conn = try zqlite.open(allocator, "mydata.db");
defer conn.close();

// In-memory database
var mem_conn = try zqlite.openMemory(allocator);
defer mem_conn.close();
```

### Basic Operations

```zig
// Execute statements (CREATE, INSERT, UPDATE, DELETE)
try conn.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)");
try conn.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");

// Query with results
var result = try conn.query("SELECT * FROM users");
defer result.deinit();

while (result.next()) |row| {
    std.debug.print("Row: {any}\n", .{row});
}
```

### Prepared Statements

```zig
var stmt = try conn.prepare("INSERT INTO users VALUES (?, ?, ?)");
defer stmt.deinit();

try stmt.bind(1, .{ .Integer = 1 });
try stmt.bind(2, .{ .Text = "Alice" });
try stmt.bind(3, .{ .Text = "alice@example.com" });
try stmt.execute();

// Reuse
try stmt.reset();
try stmt.bind(1, .{ .Integer = 2 });
// ...
```

### Transactions

```zig
try conn.execute("BEGIN TRANSACTION");
// ... operations ...
try conn.execute("COMMIT");
// or: try conn.execute("ROLLBACK");
```

### Connection Pooling

```zig
var pool = try zqlite.createConnectionPool(allocator, "mydata.db", 4, 16);
defer pool.deinit();

var conn = try pool.acquire();
defer pool.release(conn);
```

## Value Types

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

## Error Handling

```zig
conn.execute("...") catch |err| switch (err) {
    error.ParseError => // Invalid SQL
    error.TableNotFound => // Table doesn't exist
    error.ConstraintViolation => // Constraint violated
    error.OutOfMemory => // Allocation failed
    else => return err,
};
```

## SQL Support

### Supported Statements
- `CREATE TABLE` with constraints (PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT)
- `INSERT`, `UPDATE`, `DELETE`
- `SELECT` with WHERE, ORDER BY, LIMIT
- `BEGIN`, `COMMIT`, `ROLLBACK`
- `CREATE INDEX`, `DROP TABLE`

### Data Types
- `INTEGER` - 64-bit signed integer
- `REAL` - 64-bit floating point
- `TEXT` - UTF-8 string
- `BLOB` - Binary data
- `BOOLEAN` - True/false (stored as INTEGER)

## Performance Tips

1. **Use transactions for bulk operations**
2. **Reuse prepared statements**
3. **Create indexes on frequently queried columns**
4. **Use connection pooling for concurrent access**
5. **Use `defer` to ensure resource cleanup**

## Full Example

```zig
const std = @import("std");
const zqlite = @import("zqlite");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn = try zqlite.open(allocator, "app.db");
    defer conn.close();

    try conn.execute(
        \\CREATE TABLE IF NOT EXISTS users (
        \\  id INTEGER PRIMARY KEY,
        \\  name TEXT NOT NULL,
        \\  email TEXT UNIQUE
        \\)
    );

    var stmt = try conn.prepare("INSERT INTO users (name, email) VALUES (?, ?)");
    defer stmt.deinit();

    try stmt.bind(1, .{ .Text = "Alice" });
    try stmt.bind(2, .{ .Text = "alice@example.com" });
    try stmt.execute();

    var result = try conn.query("SELECT * FROM users");
    defer result.deinit();

    while (result.next()) |row| {
        std.debug.print("{any}\n", .{row});
    }
}
```
