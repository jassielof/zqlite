const std = @import("std");
const zqlite = @import("zqlite");

/// Benchmark validator for CI regression detection
/// Runs benchmarks and validates against baseline thresholds
const BenchResult = struct {
    name: []const u8,
    ops_per_sec: f64,
    min_threshold: f64,
    passed: bool,
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    std.debug.print("\n🔍 ZQLite Benchmark Validation (v1.3.3)\n", .{});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    var conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE bench (id INTEGER, value TEXT)");

    var results = std.array_list.Managed(BenchResult).init(allocator);
    defer results.deinit();

    // Benchmark 1: Simple INSERTs
    {
        const num_ops: usize = 10;
        const ts_start = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const start = @as(i128, ts_start.sec) * std.time.ns_per_s + ts_start.nsec;

        var i: usize = 0;
        while (i < num_ops) : (i += 1) {
            try conn.execute("INSERT INTO bench (id, value) VALUES (1, 'test')");
        }

        const ts_end = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const end_time = @as(i128, ts_end.sec) * std.time.ns_per_s + ts_end.nsec;
        const duration_s = @as(f64, @floatFromInt(end_time - start)) / 1_000_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(num_ops)) / duration_s;
        const min_threshold = 3000.0;

        try results.append(.{
            .name = "Simple INSERT",
            .ops_per_sec = ops_per_sec,
            .min_threshold = min_threshold,
            .passed = ops_per_sec >= min_threshold,
        });
    }

    // Benchmark 2: Bulk INSERTs
    {
        const num_ops: usize = 10;
        const ts_start = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const start = @as(i128, ts_start.sec) * std.time.ns_per_s + ts_start.nsec;

        try conn.execute("BEGIN TRANSACTION");
        var i: usize = 0;
        while (i < num_ops) : (i += 1) {
            try conn.execute("INSERT INTO bench (id, value) VALUES (2, 'bulk')");
        }
        try conn.execute("COMMIT");

        const ts_end = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const end_time = @as(i128, ts_end.sec) * std.time.ns_per_s + ts_end.nsec;
        const duration_s = @as(f64, @floatFromInt(end_time - start)) / 1_000_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(num_ops)) / duration_s;
        const min_threshold = 2000.0;

        try results.append(.{
            .name = "Bulk INSERT",
            .ops_per_sec = ops_per_sec,
            .min_threshold = min_threshold,
            .passed = ops_per_sec >= min_threshold,
        });
    }

    // Benchmark 3: SELECT queries
    {
        const num_ops: usize = 50;
        const ts_start = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const start = @as(i128, ts_start.sec) * std.time.ns_per_s + ts_start.nsec;

        var i: usize = 0;
        while (i < num_ops) : (i += 1) {
            var result = try conn.query("SELECT * FROM bench");
            result.deinit();
        }

        const ts_end = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const end_time = @as(i128, ts_end.sec) * std.time.ns_per_s + ts_end.nsec;
        const duration_s = @as(f64, @floatFromInt(end_time - start)) / 1_000_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(num_ops)) / duration_s;
        const min_threshold = 2000.0;

        try results.append(.{
            .name = "SELECT query",
            .ops_per_sec = ops_per_sec,
            .min_threshold = min_threshold,
            .passed = ops_per_sec >= min_threshold,
        });
    }

    // Benchmark 4: UPDATEs
    {
        const num_ops: usize = 50;
        const ts_start = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const start = @as(i128, ts_start.sec) * std.time.ns_per_s + ts_start.nsec;

        var i: usize = 0;
        while (i < num_ops) : (i += 1) {
            try conn.execute("UPDATE bench SET value = 'updated' WHERE id = 1");
        }

        const ts_end = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const end_time = @as(i128, ts_end.sec) * std.time.ns_per_s + ts_end.nsec;
        const duration_s = @as(f64, @floatFromInt(end_time - start)) / 1_000_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(num_ops)) / duration_s;
        const min_threshold = 150.0;

        try results.append(.{
            .name = "UPDATE",
            .ops_per_sec = ops_per_sec,
            .min_threshold = min_threshold,
            .passed = ops_per_sec >= min_threshold,
        });
    }

    // Print results
    std.debug.print("Benchmark Results:\n", .{});
    std.debug.print("-" ** 80 ++ "\n", .{});

    var all_passed = true;
    for (results.items) |result| {
        const status = if (result.passed) "✅ PASS" else "❌ FAIL";
        std.debug.print("{s} {s:<20} {d:>8.0} ops/sec (min: {d:>8.0})\n", .{
            status,
            result.name,
            result.ops_per_sec,
            result.min_threshold,
        });
        if (!result.passed) all_passed = false;
    }

    std.debug.print("=" ** 80 ++ "\n", .{});

    if (all_passed) {
        std.debug.print("✅ All benchmarks passed regression thresholds!\n\n", .{});
        std.process.exit(0);
    } else {
        std.debug.print("❌ Some benchmarks failed regression thresholds!\n\n", .{});
        std.process.exit(1);
    }
}
