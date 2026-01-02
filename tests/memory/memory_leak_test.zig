const std = @import("std");
const zqlite = @import("zqlite");

/// Dedicated memory leak detection test
/// Run with: zig build run-memory-test
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{
        .stack_trace_frames = 16, // More stack frames for debugging
    }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .ok) {
            std.debug.print("\n✅ NO MEMORY LEAKS DETECTED!\n", .{});
        } else {
            std.debug.print("\n❌ MEMORY LEAK DETECTED! Check output above.\n", .{});
            std.process.exit(1);
        }
    }
    const allocator = gpa.allocator();

    std.debug.print("🔍 ZQLite Memory Leak Detection Test\n", .{});
    std.debug.print("=====================================\n\n", .{});

    var tests_passed: u32 = 0;
    var tests_failed: u32 = 0;

    // Test 1: Basic open/close cycle
    std.debug.print("Test 1: Open/Close Cycle... ", .{});
    testOpenClose(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 2: CREATE TABLE
    std.debug.print("Test 2: CREATE TABLE... ", .{});
    testCreateTable(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 2) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 3: INSERT operations
    std.debug.print("Test 3: INSERT Operations... ", .{});
    testInsert(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 3) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 4: SELECT queries
    std.debug.print("Test 4: SELECT Queries... ", .{});
    testSelect(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 4) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 5: Prepared statements
    std.debug.print("Test 5: Prepared Statements... ", .{});
    testPreparedStatements(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 5) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 6: Transaction handling
    std.debug.print("Test 6: Transactions... ", .{});
    testTransactions(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 6) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 7: Aggregate functions
    std.debug.print("Test 7: Aggregate Functions... ", .{});
    testAggregates(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 7) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    // Test 8: Error paths (should clean up properly)
    std.debug.print("Test 8: Error Path Cleanup... ", .{});
    testErrorPaths(allocator) catch |err| {
        std.debug.print("FAILED ({})\n", .{err});
        tests_failed += 1;
    };
    if (tests_failed == 0 or tests_passed + tests_failed == 8) {
        std.debug.print("PASSED\n", .{});
        tests_passed += 1;
    }

    std.debug.print("\n=====================================\n", .{});
    std.debug.print("Results: {} passed, {} failed\n", .{ tests_passed, tests_failed });
}

fn testOpenClose(allocator: std.mem.Allocator) !void {
    for (0..10) |_| {
        const conn = try zqlite.open(allocator, ":memory:");
        conn.close();
    }
}

fn testCreateTable(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE test1 (id INTEGER, name TEXT)");
    try conn.execute("CREATE TABLE test2 (a INTEGER, b REAL, c TEXT)");
    try conn.execute("CREATE TABLE test3 (x INTEGER PRIMARY KEY, y TEXT NOT NULL)");
}

fn testInsert(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE users (id INTEGER, name TEXT)");

    for (0..100) |i| {
        var buf: [256]u8 = undefined;
        const sql = try std.fmt.bufPrint(&buf, "INSERT INTO users VALUES ({}, 'User {}')", .{ i, i });
        try conn.execute(sql);
    }
}

fn testSelect(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE data (id INTEGER, value TEXT)");
    try conn.execute("INSERT INTO data VALUES (1, 'one')");
    try conn.execute("INSERT INTO data VALUES (2, 'two')");
    try conn.execute("INSERT INTO data VALUES (3, 'three')");

    for (0..50) |_| {
        var result = try conn.query("SELECT * FROM data");
        defer result.deinit();

        while (result.next()) |row| {
            var r = row;
            defer r.deinit();
            _ = r.getValue(0);
            _ = r.getValue(1);
        }
    }
}

fn testPreparedStatements(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE items (id INTEGER, name TEXT)");

    for (0..20) |i| {
        var stmt = try conn.prepare("INSERT INTO items VALUES (?, ?)");
        defer stmt.deinit();

        // bindParameter clones the value internally, so pass literal text directly
        // Do NOT dupe manually - PreparedStatement.deinit() will free the cloned value
        try stmt.bindParameter(0, zqlite.storage.Value{ .Integer = @intCast(i) });
        try stmt.bindParameter(1, zqlite.storage.Value{ .Text = @constCast("test") });

        var result = try stmt.execute();
        result.deinit();
    }
}

fn testTransactions(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE accounts (id INTEGER, balance INTEGER)");

    // Multiple transaction cycles
    for (0..10) |i| {
        try conn.begin();
        var buf: [256]u8 = undefined;
        const sql = try std.fmt.bufPrint(&buf, "INSERT INTO accounts VALUES ({}, 1000)", .{i});
        try conn.execute(sql);
        try conn.commit();
    }
}

fn testAggregates(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE sales (region TEXT, amount INTEGER)");
    try conn.execute("INSERT INTO sales VALUES ('North', 100)");
    try conn.execute("INSERT INTO sales VALUES ('North', 200)");
    try conn.execute("INSERT INTO sales VALUES ('South', 150)");

    // Test different aggregate queries
    {
        var result = try conn.query("SELECT COUNT(*) FROM sales");
        defer result.deinit();
        while (result.next()) |row| {
            var r = row;
            r.deinit();
        }
    }
    {
        var result = try conn.query("SELECT SUM(amount) FROM sales");
        defer result.deinit();
        while (result.next()) |row| {
            var r = row;
            r.deinit();
        }
    }
    {
        var result = try conn.query("SELECT AVG(amount) FROM sales");
        defer result.deinit();
        while (result.next()) |row| {
            var r = row;
            r.deinit();
        }
    }
}

fn testErrorPaths(allocator: std.mem.Allocator) !void {
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Try invalid SQL - should cleanup properly
    _ = conn.execute("INVALID SQL STATEMENT") catch {};

    // Try querying non-existent table
    _ = conn.query("SELECT * FROM nonexistent") catch {};

    // Try creating duplicate table
    try conn.execute("CREATE TABLE dup (id INTEGER)");
    _ = conn.execute("CREATE TABLE dup (id INTEGER)") catch {};
}
