const std = @import("std");
const zqlite = @import("zqlite");
const cli = @import("zqlite").cli;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Convert [:0]u8 to []const u8
    var const_args = try allocator.alloc([]const u8, args.len);
    defer allocator.free(const_args);
    for (args, 0..) |arg, i| {
        const_args[i] = arg;
    }

    if (args.len <= 1) {
        // No arguments, start interactive shell
        try cli.runShell();
    } else {
        // Process command line arguments
        try cli.executeCommand(allocator, const_args);
    }
}

test "simple test" {
    var list: std.ArrayList(i32) = .{};
    defer list.deinit(std.testing.allocator);
    try list.append(std.testing.allocator, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "database integration" {
    // Test in-memory database
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.openMemory(allocator);
    defer conn.close();

    // Test basic functionality
    try std.testing.expect(conn.info().is_memory);

    // Test table creation
    try conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");

    // Test insertion
    try conn.execute("INSERT INTO users VALUES (1, 'Alice');");
    try conn.execute("INSERT INTO users VALUES (2, 'Bob');");

    // Test selection (this should work without crashing)
    try conn.execute("SELECT * FROM users;");

    std.debug.print("✅ Integration test passed!\n", .{});
}

test "end-to-end workflow" {
    const allocator = std.testing.allocator;

    // Create in-memory database
    const conn = try zqlite.openMemory(allocator);
    defer conn.close();

    // Create table
    try conn.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL);");

    // Insert data
    try conn.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99);");
    try conn.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99);");

    // Query data
    try conn.execute("SELECT * FROM products;");

    // Test prepared statements
    var stmt = try conn.prepare("INSERT INTO products VALUES (?, ?, ?);");
    defer stmt.deinit();

    try stmt.bindParameter(0, zqlite.storage.Value{ .Integer = 3 });
    const keyboard_text = try allocator.dupe(u8, "Keyboard");
    defer allocator.free(keyboard_text);
    try stmt.bindParameter(1, zqlite.storage.Value{ .Text = keyboard_text });
    try stmt.bindParameter(2, zqlite.storage.Value{ .Real = 79.99 });

    var result = try stmt.execute();
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.affected_rows);

    std.debug.print("✅ End-to-end test passed!\n", .{});
}

test "fuzz example" {
    const Context = struct {
        fn testOne(context: @This(), input: []const u8) anyerror!void {
            _ = context;
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(Context{}, Context.testOne, .{});
}
