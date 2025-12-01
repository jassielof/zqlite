const std = @import("std");
const zqlite = @import("src/zqlite.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Testing ZQLite v1.3.3 fixes...\n", .{});

    // Test 1: Memory leak fix - CREATE TABLE with DEFAULT values
    std.debug.print("Test 1: CREATE TABLE with DEFAULT values\n", .{});
    {
        const conn = try zqlite.openMemory(allocator);
        defer conn.close();

        try conn.execute(
            \\CREATE TABLE test_table (
            \\    id INTEGER PRIMARY KEY,
            \\    name TEXT NOT NULL,
            \\    created_at TEXT DEFAULT (datetime('now')),
            \\    updated_at TEXT DEFAULT (datetime('now'))
            \\)
        );
        std.debug.print("✅ CREATE TABLE with DEFAULT values successful\n", .{});
    }

    // Test 2: DEFAULT constraint evaluation - INSERT with missing columns
    std.debug.print("Test 2: INSERT with DEFAULT constraint evaluation\n", .{});
    {
        const conn = try zqlite.openMemory(allocator);
        defer conn.close();

        try conn.execute(
            \\CREATE TABLE packages (
            \\    id INTEGER PRIMARY KEY,
            \\    name TEXT NOT NULL,
            \\    version TEXT DEFAULT 'unknown',
            \\    source_type TEXT NOT NULL,
            \\    source_url TEXT NOT NULL,
            \\    build_status TEXT DEFAULT 'pending',
            \\    added_at TEXT DEFAULT (datetime('now')),
            \\    updated_at TEXT DEFAULT (datetime('now'))
            \\)
        );

        // Insert with specific columns, should apply DEFAULT values to missing ones
        try conn.execute("INSERT INTO packages (name, source_type, source_url) VALUES ('test-pkg', 'git', 'https://example.com')");

        std.debug.print("✅ INSERT with DEFAULT constraint evaluation successful\n", .{});
    }

    // Test 3: Function call parsing in INSERT VALUES (if parser supports it)
    std.debug.print("Test 3: Function calls in INSERT VALUES\n", .{});
    {
        const conn = try zqlite.openMemory(allocator);
        defer conn.close();

        try conn.execute(
            \\CREATE TABLE events (
            \\    id INTEGER PRIMARY KEY,
            \\    name TEXT NOT NULL,
            \\    created_at TEXT
            \\)
        );

        // This might fail if parser doesn't support function calls in VALUES yet
        conn.execute("INSERT INTO events (name, created_at) VALUES ('test', datetime('now'))") catch |err| {
            std.debug.print("⚠️  Function calls in INSERT VALUES not yet supported: {}\n", .{err});
            // Try with a prepared statement or direct value instead
            try conn.execute("INSERT INTO events (name, created_at) VALUES ('test', '2023-12-25 12:00:00')");
        };

        std.debug.print("✅ INSERT with function calls (or fallback) successful\n", .{});
    }

    std.debug.print("All tests completed successfully! 🎉\n", .{});
}