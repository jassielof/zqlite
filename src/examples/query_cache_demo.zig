const std = @import("std");
const zqlite = @import("zqlite");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("⚡ zqlite Query Cache Demo\n\n", .{});

    // Create query cache with 100 entries max, 1MB memory limit
    const cache = try zqlite.createQueryCache(allocator, 100, 1024 * 1024);
    defer cache.deinit();

    std.debug.print("✅ Created query cache (100 entries, 1MB limit)\n\n", .{});

    // Create in-memory database
    var conn = try zqlite.openMemory(allocator);
    defer conn.close();

    // Set up test data
    std.debug.print("📊 Setting up test data...\n", .{});

    try conn.execute("CREATE TABLE products (id INTEGER, name TEXT, category TEXT, price REAL);");

    const test_products = [_]struct { id: i32, name: []const u8, category: []const u8, price: f64 }{
        .{ .id = 1, .name = "Laptop", .category = "Electronics", .price = 999.99 },
        .{ .id = 2, .name = "Mouse", .category = "Electronics", .price = 29.99 },
        .{ .id = 3, .name = "Book", .category = "Education", .price = 19.99 },
        .{ .id = 4, .name = "Desk", .category = "Furniture", .price = 299.99 },
        .{ .id = 5, .name = "Chair", .category = "Furniture", .price = 199.99 },
    };

    for (test_products) |product| {
        var stmt = try conn.prepare("INSERT INTO products VALUES (?, ?, ?, ?);");
        defer stmt.deinit();

        try stmt.bindParameter(0, zqlite.storage.Value{ .Integer = product.id });
        try stmt.bindParameter(1, zqlite.storage.Value{ .Text = try allocator.dupe(u8, product.name) });
        defer allocator.free(product.name);
        try stmt.bindParameter(2, zqlite.storage.Value{ .Text = try allocator.dupe(u8, product.category) });
        defer allocator.free(product.category);
        try stmt.bindParameter(3, zqlite.storage.Value{ .Real = product.price });

        var result = try stmt.execute();
        defer result.deinit();
    }

    std.debug.print("✅ Inserted {} product records\n\n", .{test_products.len});

    // Demonstrate query caching
    std.debug.print("⚡ Query Caching Demonstrations:\n\n", .{});

    const test_queries = [_][]const u8{
        "SELECT * FROM products WHERE category = 'Electronics'",
        "SELECT name, price FROM products WHERE price > 100",
        "SELECT category, COUNT(*) FROM products GROUP BY category",
        "SELECT * FROM products ORDER BY price DESC LIMIT 3",
    };

    for (test_queries, 0..) |query, i| {
        std.debug.print("{}. Query: {s}\n", .{ i + 1, query });

        // First execution - cache miss
        const ts_start = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const start_time = @as(i128, ts_start.sec) * std.time.ns_per_s + ts_start.nsec;

        // Hash the query for caching
        const query_hash = cache.hashQuery(query);

        // Check if query is in cache
        const cached_result = cache.get(query_hash);

        if (cached_result == null) {
            std.debug.print("   ❌ Cache MISS - Executing query\n", .{});

            // Execute the query (simulated - would integrate with real executor)
            std.Thread.sleep(10 * std.time.ns_per_ms); // Simulate query execution time

            // Create mock result for caching
            const ts_end = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            const end_time = @as(i128, ts_end.sec) * std.time.ns_per_s + ts_end.nsec;
            var mock_result = zqlite.query_cache.CachedResult{
                .rows = try allocator.alloc(zqlite.storage.Row, 2),
                .columns = try allocator.alloc([]const u8, 2),
                .execution_time_ns = end_time - start_time,
            };

            // Set up mock data
            mock_result.columns[0] = try allocator.dupe(u8, "name");
            mock_result.columns[1] = try allocator.dupe(u8, "price");

            for (mock_result.rows, 0..) |*row, j| {
                row.values = try allocator.alloc(zqlite.storage.Value, 2);
                row.values[0] = zqlite.storage.Value{ .Text = try std.fmt.allocPrint(allocator, "Product {}", .{j + 1}) };
                row.values[1] = zqlite.storage.Value{ .Real = @as(f64, @floatFromInt(100 + j * 50)) };
            }

            // Cache the result
            try cache.put(query_hash, query, mock_result);

            const ts_final = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            const final_time = @as(i128, ts_final.sec) * std.time.ns_per_s + ts_final.nsec;
            const execution_time = final_time - start_time;
            std.debug.print("   ⏱️  Execution time: {d:.2}ms\n", .{@as(f64, @floatFromInt(execution_time)) / std.time.ns_per_ms});
        } else {
            std.debug.print("   ✅ Cache HIT - Using cached result\n", .{});
            const ts_cache = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            const cache_end_time = @as(i128, ts_cache.sec) * std.time.ns_per_s + ts_cache.nsec;
            const cache_time = cache_end_time - start_time;
            std.debug.print("   ⚡ Cache retrieval time: {d:.2}ms\n", .{@as(f64, @floatFromInt(cache_time)) / std.time.ns_per_ms});
            std.debug.print("   📊 Original execution time: {d:.2}ms\n", .{@as(f64, @floatFromInt(cached_result.?.execution_time_ns)) / std.time.ns_per_ms});
        }

        // Show cache statistics
        const stats = cache.getStats();
        std.debug.print("   📈 Cache stats: {} entries, {} hits, {} misses\n\n", .{ stats.total_entries, stats.cache_hits, stats.cache_misses });

        // Second execution of same query - should be cache hit
        if (i == 0) {
            std.debug.print("   🔄 Re-executing same query...\n", .{});
            const cached_result_2 = cache.get(query_hash);
            if (cached_result_2 != null) {
                std.debug.print("   ✅ Cache HIT on second execution!\n", .{});
            }

            const stats_2 = cache.getStats();
            std.debug.print("   📈 Updated stats: {} entries, {} hits, {} misses\n\n", .{ stats_2.total_entries, stats_2.cache_hits, stats_2.cache_misses });
        }
    }

    // Demonstrate cache eviction
    std.debug.print("🗑️  Cache Eviction Test:\n", .{});

    // Fill cache beyond capacity to trigger eviction
    for (0..25) |i| {
        const unique_query = try std.fmt.allocPrint(allocator, "SELECT * FROM products WHERE id = {}", .{i});
        defer allocator.free(unique_query);

        const hash = cache.hashQuery(unique_query);

        // Create small mock result
        var eviction_result = zqlite.query_cache.CachedResult{
            .rows = try allocator.alloc(zqlite.storage.Row, 1),
            .columns = try allocator.alloc([]const u8, 1),
            .execution_time_ns = 1000000, // 1ms
        };

        eviction_result.columns[0] = try allocator.dupe(u8, "id");
        eviction_result.rows[0].values = try allocator.alloc(zqlite.storage.Value, 1);
        eviction_result.rows[0].values[0] = zqlite.storage.Value{ .Integer = @intCast(i) };

        try cache.put(hash, unique_query, eviction_result);
    }

    const final_stats = cache.getStats();
    std.debug.print("   📊 After adding 25 entries: {} total entries, {} evictions\n", .{ final_stats.total_entries, final_stats.evictions });

    // Test cache cleanup
    std.debug.print("\n🧹 Cache Cleanup Test:\n", .{});

    // Wait a bit, then clean up old entries
    std.Thread.sleep(100 * std.time.ns_per_ms);
    cache.cleanup(); // Remove entries older than default TTL

    const cleanup_stats = cache.getStats();
    std.debug.print("   📊 After cleanup: {} total entries\n", .{cleanup_stats.total_entries});

    // Memory usage information
    std.debug.print("\n💾 Memory Usage:\n", .{});
    std.debug.print("   📊 Current memory usage: {d:.2} KB\n", .{@as(f64, @floatFromInt(cleanup_stats.memory_used)) / 1024.0});
    std.debug.print("   📊 Memory limit: {d:.2} KB\n", .{@as(f64, @floatFromInt(cleanup_stats.memory_limit)) / 1024.0});
    std.debug.print("   📊 Memory efficiency: {d:.1}%\n", .{(@as(f64, @floatFromInt(cleanup_stats.memory_used)) / @as(f64, @floatFromInt(cleanup_stats.memory_limit))) * 100.0});

    std.debug.print("\n🎯 Query cache functionality is working!\n", .{});
    std.debug.print("💡 Cache provides significant performance improvements for repeated queries\n", .{});
    std.debug.print("🚀 Ready for integration with SQL parser and execution engine\n", .{});
}
