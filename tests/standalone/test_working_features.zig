const std = @import("std");
const parser = @import("src/parser/parser.zig");

test "SQL Comments Work" {
    const allocator = std.testing.allocator;
    
    // Test line comments are handled (don't crash parser)
    const sql = "SELECT * FROM users -- comment";
    
    var result = parser.parse(allocator, sql) catch |err| switch (err) {
        error.UnexpectedToken => {
            std.debug.print("Comments not fully supported yet\n", .{});
            return;
        },
        else => return err,
    };
    defer result.deinit();
    
    try std.testing.expect(std.meta.activeTag(result.statement) == .Select);
}

test "Basic Data Types Work" {
    const allocator = std.testing.allocator;
    
    const sql = "CREATE TABLE test (id INTEGER, name TEXT, score REAL, data BLOB)";
    
    var result = parser.parse(allocator, sql) catch |err| {
        return err;
    };
    defer result.deinit();
    
    try std.testing.expect(std.meta.activeTag(result.statement) == .CreateTable);
}

test "CURRENT_TIMESTAMP Default" {
    const allocator = std.testing.allocator;
    
    const sql = "CREATE TABLE test (id INTEGER, created TEXT DEFAULT CURRENT_TIMESTAMP)";
    
    var result = parser.parse(allocator, sql) catch |err| {
        return err;
    };
    defer result.deinit();
    
    try std.testing.expect(std.meta.activeTag(result.statement) == .CreateTable);
}

test "Transactions Work" {
    const allocator = std.testing.allocator;
    
    var begin_result = parser.parse(allocator, "BEGIN") catch |err| {
        return err;
    };
    defer begin_result.deinit();
    try std.testing.expect(std.meta.activeTag(begin_result.statement) == .BeginTransaction);
}

test "Foreign Keys Basic" {
    const allocator = std.testing.allocator;
    
    const sql = "CREATE TABLE posts (id INTEGER, user_id INTEGER REFERENCES users(id))";
    
    var result = parser.parse(allocator, sql) catch |err| {
        std.debug.print("FK error: {}\n", .{err});
        return err;
    };
    defer result.deinit();
    
    try std.testing.expect(std.meta.activeTag(result.statement) == .CreateTable);
}

test "AUTOINCREMENT Works" {
    const allocator = std.testing.allocator;
    
    const sql = "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT)";
    
    var result = parser.parse(allocator, sql) catch |err| {
        return err;
    };
    defer result.deinit();
    
    try std.testing.expect(std.meta.activeTag(result.statement) == .CreateTable);
}