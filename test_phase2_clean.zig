const std = @import("std");
const testing = std.testing;

// Import all Phase 2 components
const storage = @import("src/db/storage.zig");
const mvcc = @import("src/concurrent/mvcc_transactions.zig");
const batch_processor = @import("src/concurrent/batch_processor.zig");
const deterministic_engine = @import("src/concurrent/deterministic_engine.zig");
const hot_standby = @import("src/concurrent/hot_standby.zig");
const cluster_manager = @import("src/cluster/manager.zig");
const distributed_query = @import("src/distributed/query_engine.zig");

/// Comprehensive Phase 2 Integration Test Suite
/// Tests all advanced concurrent processing capabilities
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    std.log.info("Starting ZQLite Phase 2 Integration Tests...", .{});
    
    // Test 1: Batch Processing with 8K+ operations
    try testBatchProcessing(allocator);
    
    // Test 2: Deterministic Engine for reproducible results
    try testDeterministicEngine(allocator);
    
    // Test 3: Hot Standby for zero-downtime failover
    try testHotStandby(allocator);
    
    // Test 4: Cluster Manager for horizontal scaling
    try testClusterManager(allocator);
    
    // Test 5: Distributed Query Engine
    try testDistributedQueryEngine(allocator);
    
    // Test 6: Full Integration Test
    try testFullIntegration(allocator);
    
    std.log.info("All Phase 2 Integration Tests Passed!", .{});
}

/// Test batch processing with 8K+ operations
fn testBatchProcessing(allocator: std.mem.Allocator) !void {
    std.log.info("Testing Batch Processing with 8K+ operations...", .{});
    
    var storage_engine = try storage.StorageEngine.initMemory(allocator);
    defer storage_engine.deinit();
    
    var mvcc_manager = try mvcc.MVCCTransactionManager.init(allocator, &storage_engine, null);
    defer mvcc_manager.deinit();
    
    var batch_proc = try batch_processor.BatchProcessor.init(allocator, &mvcc_manager, 8192);
    defer batch_proc.deinit();
    
    // Create 8K+ operations
    const batch_size = 8192;
    const operations = try allocator.alloc(batch_processor.BatchOperation, batch_size);
    defer allocator.free(operations);
    
    // Fill with insert operations
    for (operations, 0..) |*op, i| {
        op.* = batch_processor.BatchOperation{
            .operation_type = .Insert,
            .table = "test_table",
            .row_id = @intCast(i + 1),
            .data = &[_]u8{1, 0, 0, 0, 0, @intCast(i & 0xFF), 0, 0, 0, 0, 0, 0, 0},
        };
    }
    
    // Process batch
    const result = try batch_proc.processBatch(operations);
    defer result.deinit(allocator);
    
    // Verify results
    if (result.successful_operations != batch_size) {
        std.log.err("Expected {d} successful operations, got {d}", .{ batch_size, result.successful_operations });
        return error.TestFailed;
    }
    
    if (result.throughput_ops_per_sec < 1000) {
        std.log.err("Throughput too low: {d} ops/sec", .{result.throughput_ops_per_sec});
        return error.TestFailed;
    }
    
    // Test metrics
    const metrics = batch_proc.getMetrics();
    if (metrics.total_operations != batch_size) {
        std.log.err("Metrics error: expected {d} total operations, got {d}", .{ batch_size, metrics.total_operations });
        return error.TestFailed;
    }
    
    std.log.info("Batch Processing Test Passed - {d} operations in {d}us ({d} ops/sec)", .{
        result.successful_operations,
        result.execution_time_us,
        result.throughput_ops_per_sec,
    });
}

/// Test deterministic engine for reproducible results
fn testDeterministicEngine(allocator: std.mem.Allocator) !void {
    std.log.info("Testing Deterministic Engine...", .{});
    
    var storage_engine = try storage.StorageEngine.initMemory(allocator);
    defer storage_engine.deinit();
    
    var mvcc_manager = try mvcc.MVCCTransactionManager.init(allocator, &storage_engine, null);
    defer mvcc_manager.deinit();
    
    // Create two engines with same seed
    var engine1 = try deterministic_engine.DeterministicEngine.init(allocator, &mvcc_manager, 12345);
    defer engine1.deinit();
    
    var engine2 = try deterministic_engine.DeterministicEngine.init(allocator, &mvcc_manager, 12345);
    defer engine2.deinit();
    
    // Test operations
    const operation = deterministic_engine.DeterministicOperation{
        .operation_type = .Insert,
        .table = "test_table",
        .row_id = 1,
        .input = &[_]u8{1, 0, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 0},
    };
    
    const result1 = try engine1.executeOperation(operation);
    const result2 = try engine2.executeOperation(operation);
    
    // Verify deterministic behavior
    if (result1.operation_id != result2.operation_id) {
        std.log.err("Non-deterministic operation IDs: {d} vs {d}", .{ result1.operation_id, result2.operation_id });
        return error.TestFailed;
    }
    
    if (result1.execution_hash != result2.execution_hash) {
        std.log.err("Non-deterministic execution hashes: {d} vs {d}", .{ result1.execution_hash, result2.execution_hash });
        return error.TestFailed;
    }
    
    // Test deterministic random
    const random1 = engine1.deterministicRandom();
    const random2 = engine2.deterministicRandom();
    
    if (random1 != random2) {
        std.log.err("Non-deterministic random numbers: {d} vs {d}", .{ random1, random2 });
        return error.TestFailed;
    }
    
    std.log.info("Deterministic Engine Test Passed - Reproducible results verified", .{});
}

/// Test hot standby for zero-downtime failover
fn testHotStandby(allocator: std.mem.Allocator) !void {
    std.log.info("Testing Hot Standby...", .{});
    
    var storage_engine = try storage.StorageEngine.initMemory(allocator);
    defer storage_engine.deinit();
    
    var mvcc_manager = try mvcc.MVCCTransactionManager.init(allocator, &storage_engine, null);
    defer mvcc_manager.deinit();
    
    var standby = try hot_standby.HotStandby.init(allocator, &mvcc_manager, "node1");
    defer standby.deinit();
    
    // Test replication
    const entry = hot_standby.ReplicationEntry{
        .index = 1,
        .timestamp = blk: {
                const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                break :blk ts.sec;
            },
        .operation_type = .Insert,
        .table = "test_table",
        .row_id = 1,
        .data = &[_]u8{1, 0, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 0},
    };
    
    try standby.handleReplicationEntry(entry);
    
    // Verify metrics
    const metrics = standby.getMetrics();
    if (metrics.entries_replicated != 1) {
        std.log.err("Expected 1 replicated entry, got {d}", .{metrics.entries_replicated});
        return error.TestFailed;
    }
    
    // Test failover (simulate low lag)
    standby.sync_state.lag_ms = 100;
    try standby.promoteToPrimary();
    
    if (standby.role != .Primary) {
        std.log.err("Expected Primary role after promotion, got {s}", .{@tagName(standby.role)});
        return error.TestFailed;
    }
    
    const updated_metrics = standby.getMetrics();
    if (updated_metrics.failover_count != 1) {
        std.log.err("Expected 1 failover, got {d}", .{updated_metrics.failover_count});
        return error.TestFailed;
    }
    
    std.log.info("Hot Standby Test Passed - Failover completed successfully", .{});
}

/// Test cluster manager for horizontal scaling
fn testClusterManager(allocator: std.mem.Allocator) !void {
    std.log.info("Testing Cluster Manager...", .{});
    
    var cluster = try cluster_manager.ClusterManager.init(allocator, "test_cluster", "node1");
    defer cluster.deinit();
    
    // Test adding nodes
    try cluster.addNode("node2", "127.0.0.1", 8081);
    try cluster.addNode("node3", "127.0.0.1", 8082);
    
    var metrics = cluster.getMetrics();
    if (metrics.active_nodes != 3) {
        std.log.err("Expected 3 active nodes, got {d}", .{metrics.active_nodes});
        return error.TestFailed;
    }
    
    if (metrics.nodes_added != 2) {
        std.log.err("Expected 2 nodes added, got {d}", .{metrics.nodes_added});
        return error.TestFailed;
    }
    
    // Test scaling up
    try cluster.scaleUp(5);
    metrics = cluster.getMetrics();
    if (metrics.active_nodes != 5) {
        std.log.err("Expected 5 active nodes after scale up, got {d}", .{metrics.active_nodes});
        return error.TestFailed;
    }
    
    if (metrics.scale_up_operations != 1) {
        std.log.err("Expected 1 scale up operation, got {d}", .{metrics.scale_up_operations});
        return error.TestFailed;
    }
    
    // Test scaling down
    try cluster.scaleDown(3);
    metrics = cluster.getMetrics();
    if (metrics.active_nodes != 3) {
        std.log.err("Expected 3 active nodes after scale down, got {d}", .{metrics.active_nodes});
        return error.TestFailed;
    }
    
    if (metrics.scale_down_operations != 1) {
        std.log.err("Expected 1 scale down operation, got {d}", .{metrics.scale_down_operations});
        return error.TestFailed;
    }
    
    // Test cluster health
    const health = cluster.getClusterHealth();
    if (health.total_nodes < 1) {
        std.log.err("Expected at least 1 node in cluster health, got {d}", .{health.total_nodes});
        return error.TestFailed;
    }
    
    std.log.info("Cluster Manager Test Passed - Scaling operations completed successfully", .{});
}

/// Test distributed query engine
fn testDistributedQueryEngine(allocator: std.mem.Allocator) !void {
    std.log.info("Testing Distributed Query Engine...", .{});
    
    var cluster = try cluster_manager.ClusterManager.init(allocator, "test_cluster", "node1");
    defer cluster.deinit();
    
    var dist_engine = try distributed_query.DistributedQueryEngine.init(allocator, &cluster);
    defer dist_engine.deinit();
    
    // Test simple query
    const result = try dist_engine.executeQuery("SELECT * FROM test_table");
    if (!result.success) {
        std.log.err("Query execution failed");
        return error.TestFailed;
    }
    
    // Test metrics
    const metrics = dist_engine.getMetrics();
    if (metrics.queries_executed != 1) {
        std.log.err("Expected 1 query executed, got {d}", .{metrics.queries_executed});
        return error.TestFailed;
    }
    
    if (metrics.average_execution_time_us == 0) {
        std.log.err("Expected non-zero average execution time");
        return error.TestFailed;
    }
    
    // Test batch execution
    const queries = [_][]const u8{
        "SELECT * FROM table1",
        "SELECT * FROM table2",
        "SELECT * FROM table3",
    };
    
    const batch_results = try dist_engine.executeBatch(&queries);
    defer allocator.free(batch_results);
    
    if (batch_results.len != 3) {
        std.log.err("Expected 3 batch results, got {d}", .{batch_results.len});
        return error.TestFailed;
    }
    
    for (batch_results) |batch_result| {
        if (!batch_result.success) {
            std.log.err("Batch query failed");
            return error.TestFailed;
        }
    }
    
    const updated_metrics = dist_engine.getMetrics();
    if (updated_metrics.batch_queries_executed != 1) {
        std.log.err("Expected 1 batch query executed, got {d}", .{updated_metrics.batch_queries_executed});
        return error.TestFailed;
    }
    
    std.log.info("Distributed Query Engine Test Passed - Query distribution working correctly", .{});
}

/// Test full integration of all Phase 2 components
fn testFullIntegration(allocator: std.mem.Allocator) !void {
    std.log.info("Testing Full Phase 2 Integration...", .{});
    
    // Initialize all components
    var storage_engine = try storage.StorageEngine.initMemory(allocator);
    defer storage_engine.deinit();
    
    var mvcc_manager = try mvcc.MVCCTransactionManager.init(allocator, &storage_engine, null);
    defer mvcc_manager.deinit();
    
    var batch_proc = try batch_processor.BatchProcessor.init(allocator, &mvcc_manager, 1000);
    defer batch_proc.deinit();
    
    var det_engine = try deterministic_engine.DeterministicEngine.init(allocator, &mvcc_manager, 12345);
    defer det_engine.deinit();
    
    var standby = try hot_standby.HotStandby.init(allocator, &mvcc_manager, "node1");
    defer standby.deinit();
    
    var cluster = try cluster_manager.ClusterManager.init(allocator, "test_cluster", "node1");
    defer cluster.deinit();
    
    var dist_engine = try distributed_query.DistributedQueryEngine.init(allocator, &cluster);
    defer dist_engine.deinit();
    
    // Test coordinated operations
    
    // 1. Insert data using batch processor
    const operations = [_]batch_processor.BatchOperation{
        .{
            .operation_type = .Insert,
            .table = "test_table",
            .row_id = 1,
            .data = &[_]u8{1, 0, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 0},
        },
        .{
            .operation_type = .Insert,
            .table = "test_table",
            .row_id = 2,
            .data = &[_]u8{1, 0, 0, 0, 0, 124, 0, 0, 0, 0, 0, 0, 0},
        },
    };
    
    const batch_result = try batch_proc.processBatch(&operations);
    defer batch_result.deinit(allocator);
    
    if (batch_result.successful_operations != 2) {
        std.log.err("Expected 2 successful batch operations, got {d}", .{batch_result.successful_operations});
        return error.TestFailed;
    }
    
    // 2. Execute deterministic operations
    const det_operation = deterministic_engine.DeterministicOperation{
        .operation_type = .Select,
        .table = "test_table",
        .row_id = 1,
        .input = &[_]u8{},
    };
    
    const det_result = try det_engine.executeOperation(det_operation);
    if (!det_result.result.success) {
        std.log.err("Deterministic operation failed");
        return error.TestFailed;
    }
    
    // 3. Replicate to standby
    const replication_entry = hot_standby.ReplicationEntry{
        .index = 1,
        .timestamp = blk: {
                const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                break :blk ts.sec;
            },
        .operation_type = .Insert,
        .table = "test_table",
        .row_id = 3,
        .data = &[_]u8{1, 0, 0, 0, 0, 125, 0, 0, 0, 0, 0, 0, 0},
    };
    
    try standby.handleReplicationEntry(replication_entry);
    
    // 4. Scale cluster
    try cluster.addNode("node2", "127.0.0.1", 8081);
    
    // 5. Execute distributed query
    const query_result = try dist_engine.executeQuery("SELECT * FROM test_table");
    if (!query_result.success) {
        std.log.err("Distributed query failed");
        return error.TestFailed;
    }
    
    // Verify all components working together
    const batch_metrics = batch_proc.getMetrics();
    const det_state = det_engine.getDeterministicState();
    const standby_metrics = standby.getMetrics();
    const cluster_metrics = cluster.getMetrics();
    const dist_metrics = dist_engine.getMetrics();
    
    if (batch_metrics.total_operations == 0) {
        std.log.err("No batch operations recorded");
        return error.TestFailed;
    }
    
    if (det_state.execution_count == 0) {
        std.log.err("No deterministic executions recorded");
        return error.TestFailed;
    }
    
    if (standby_metrics.entries_replicated == 0) {
        std.log.err("No replication entries recorded");
        return error.TestFailed;
    }
    
    if (cluster_metrics.active_nodes < 2) {
        std.log.err("Cluster scaling failed");
        return error.TestFailed;
    }
    
    if (dist_metrics.queries_executed == 0) {
        std.log.err("No distributed queries executed");
        return error.TestFailed;
    }
    
    std.log.info("Full Integration Test Passed - All Phase 2 components working together", .{});
    
    // Print summary
    std.log.info("Phase 2 Summary:", .{});
    std.log.info("  - Batch Operations: {d}", .{batch_metrics.total_operations});
    std.log.info("  - Deterministic Executions: {d}", .{det_state.execution_count});
    std.log.info("  - Replication Entries: {d}", .{standby_metrics.entries_replicated});
    std.log.info("  - Cluster Nodes: {d}", .{cluster_metrics.active_nodes});
    std.log.info("  - Distributed Queries: {d}", .{dist_metrics.queries_executed});
    std.log.info("  - Average Query Time: {d}us", .{dist_metrics.average_execution_time_us});
    std.log.info("  - Throughput: {d} ops/sec", .{batch_result.throughput_ops_per_sec});
}

// Run tests
test "Phase 2 Integration Suite" {
    try main();
}