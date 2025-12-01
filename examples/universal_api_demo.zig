const std = @import("std");
const zqlite = @import("zqlite");

/// ZQLite Universal API Demo
/// Shows how non-crypto applications can leverage zqlite's broad API surfaces
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("🚀 {s} - Universal Database API Demo\n", .{zqlite.version.FULL_VERSION_STRING});
    std.debug.print("   SQLite watch out! Here comes the competition! 🏆\n\n", .{});

    // Test 1: Basic CRUD operations with new API
    std.debug.print("📊 Test 1: CRUD Operations with New API\n", .{});

    var conn = try zqlite.openMemory(allocator);
    defer conn.close();

    // Create users table
    const create_sql =
        \\CREATE TABLE users (
        \\    id INTEGER PRIMARY KEY,
        \\    name TEXT NOT NULL,
        \\    email TEXT,
        \\    age INTEGER DEFAULT 18,
        \\    salary REAL
        \\)
    ;

    const affected = try conn.exec(create_sql);
    std.debug.print("   ✅ Created users table (affected: {d})\n", .{affected});

    // Insert some data
    const insert_affected = try conn.exec("INSERT INTO users (name, email, age, salary) VALUES ('Alice', 'alice@example.com', 30, 75000.50)");
    try conn.execute("INSERT INTO users (name, email, age, salary) VALUES ('Bob', 'bob@example.com', 25, 65000.00)");
    try conn.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)");
    std.debug.print("   ✅ Inserted users (first insert affected: {d})\n", .{insert_affected});

    // Test 2: Query with ResultSet API
    std.debug.print("\n🔍 Test 2: Query with ResultSet API\n", .{});

    var result_set = try conn.query("SELECT * FROM users");
    defer result_set.deinit();

    std.debug.print("   📋 Found {d} users with {d} columns:\n", .{ result_set.count(), result_set.columnCount() });

    // Print column headers
    std.debug.print("   │ ", .{});
    for (0..result_set.columnCount()) |i| {
        if (result_set.columnName(i)) |col_name| {
            std.debug.print("{s:>12} │ ", .{col_name});
        }
    }
    std.debug.print("\n   ├", .{});
    for (0..result_set.columnCount()) |_| {
        std.debug.print("──────────────┼", .{});
    }
    std.debug.print("\n", .{});

    // Iterate through results with type-safe access
    while (result_set.next()) |row| {
        std.debug.print("   │ ", .{});

        // Access by column name (type-safe)
        if (row.getIntByName("id")) |id| {
            std.debug.print("{d:>12} │ ", .{id});
        } else {
            std.debug.print("{s:>12} │ ", .{"NULL"});
        }

        if (row.getTextByName("name")) |name| {
            std.debug.print("{s:>12} │ ", .{name});
        } else {
            std.debug.print("{s:>12} │ ", .{"NULL"});
        }

        if (row.getTextByName("email")) |email| {
            std.debug.print("{s:>12} │ ", .{email});
        } else {
            std.debug.print("{s:>12} │ ", .{"NULL"});
        }

        if (row.getIntByName("age")) |age| {
            std.debug.print("{d:>12} │ ", .{age});
        } else {
            std.debug.print("{s:>12} │ ", .{"NULL"});
        }

        if (row.getRealByName("salary")) |salary| {
            std.debug.print("{d:>12.2} │ ", .{salary});
        } else {
            std.debug.print("{s:>12} │ ", .{"NULL"});
        }

        std.debug.print("\n", .{});
    }

    // Test 3: Single row query
    std.debug.print("\n👤 Test 3: Single Row Query\n", .{});

    if (try conn.queryRow("SELECT name, age FROM users WHERE age > 28")) |row| {
        const name = row.getTextByName("name") orelse "Unknown";
        const age = row.getIntByName("age") orelse 0;
        std.debug.print("   ✅ Found user: {s}, age {d}\n", .{ name, age });
    } else {
        std.debug.print("   ❌ No users found over 28\n", .{});
    }

    // Test 4: Schema introspection
    std.debug.print("\n🔍 Test 4: Schema Introspection\n", .{});

    if (try conn.getTableSchema("users")) |schema| {
        var mutable_schema = schema;
        defer mutable_schema.deinit();

        std.debug.print("   📋 Table '{s}' has {d} columns:\n", .{ schema.table_name, schema.columnCount() });

        for (schema.columns, 0..) |column, i| {
            std.debug.print("   │ {d}. {s} ({any}) - PK: {}, Nullable: {}, Default: {}\n", .{
                i + 1,
                column.name,
                column.data_type,
                column.is_primary_key,
                column.is_nullable,
                column.has_default,
            });
        }
    }

    // Test 5: List all tables
    std.debug.print("\n📁 Test 5: List All Tables\n", .{});

    const table_names = try conn.getTableNames();
    defer {
        // Note: getTableNames() now uses the same allocator as the connection
        for (table_names) |name| {
            allocator.free(name);
        }
        allocator.free(table_names);
    }

    std.debug.print("   📋 Found {d} table(s):\n", .{table_names.len});
    for (table_names, 0..) |name, i| {
        std.debug.print("   │ {d}. {s}\n", .{ i + 1, name });
    }

    // Test 6: Advanced queries
    std.debug.print("\n⚡ Test 6: Advanced Queries\n", .{});

    // Update with exec() return value
    const updated_count = try conn.exec("UPDATE users SET salary = 80000.00 WHERE age > 30");
    std.debug.print("   ✅ Updated {d} users' salaries\n", .{updated_count});

    // Complex query
    var salary_result = try conn.query("SELECT name, salary FROM users WHERE salary IS NOT NULL ORDER BY salary DESC");
    defer salary_result.deinit();

    std.debug.print("   💰 Users by salary (highest first):\n", .{});
    while (salary_result.next()) |row| {
        const name = row.getTextByName("name") orelse "Unknown";
        if (row.getRealByName("salary")) |salary| {
            std.debug.print("   │ {s}: ${d:.2}\n", .{ name, salary });
        }
    }

    std.debug.print("\n🎉 {s} Universal API Demo Complete!\n", .{zqlite.version.FULL_VERSION_STRING});
    std.debug.print("   ✨ Perfect for any Zig application - crypto or not! ✨\n", .{});
    std.debug.print("   🏆 SQLite compatibility with modern Zig ergonomics! 🏆\n", .{});
}
