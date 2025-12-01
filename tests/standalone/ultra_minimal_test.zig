const std = @import("std");
const zqlite = @import("src/zqlite.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("❌ MEMORY LEAKS DETECTED!\n", .{});
            std.process.exit(1);
        } else {
            std.debug.print("✅ NO MEMORY LEAKS!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    std.debug.print("🔍 Ultra Minimal Test - Just Table Creation + Select\n", .{});

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Create table
    try conn.execute("CREATE TABLE t (id INTEGER, name TEXT DEFAULT 'test')");

    // Insert just one row
    try conn.execute("INSERT INTO t (id) VALUES (1)");

    // Select just one row
    var stmt = try conn.prepare("SELECT id, name FROM t");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit();

    std.debug.print("✅ Found {} rows\n", .{result.rows.items.len});
}