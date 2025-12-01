const std = @import("std");
const zqlite = @import("src/zqlite.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("🧪 Testing getTableNames() allocation/deallocation\n", .{});

    // Create connection
    var conn = try zqlite.openMemory(allocator);
    defer conn.close();

    // Create a simple table
    try conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT)");
    std.debug.print("   ✅ Created test_table\n", .{});

    // Get table names
    const table_names = try conn.getTableNames();
    std.debug.print("   📋 Found {d} table(s)\n", .{table_names.len});

    for (table_names) |name| {
        std.debug.print("   │ {s}\n", .{name});
    }

    // Free table names manually
    std.debug.print("   🗑️  Freeing table names...\n", .{});
    for (table_names) |name| {
        allocator.free(name);
    }
    allocator.free(table_names);

    std.debug.print("   ✅ getTableNames() test complete!\n", .{});
}