const std = @import("std");
const storage = @import("../db/storage.zig");
const ast = @import("../parser/ast.zig");

/// Window function execution context
pub const WindowContext = struct {
    allocator: std.mem.Allocator,
    rows: []storage.Row,
    current_row: usize,
    partition_start: usize,
    partition_end: usize,

    pub fn init(allocator: std.mem.Allocator, rows: []storage.Row) WindowContext {
        return WindowContext{
            .allocator = allocator,
            .rows = rows,
            .current_row = 0,
            .partition_start = 0,
            .partition_end = rows.len,
        };
    }
};

/// Window function executor
pub const WindowExecutor = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) WindowExecutor {
        return WindowExecutor{ .allocator = allocator };
    }

    /// Execute a window function
    pub fn executeWindowFunction(
        self: *WindowExecutor,
        function: ast.WindowFunction,
        context: *WindowContext,
    ) !storage.Value {
        return switch (function.function_type) {
            .RowNumber => try self.executeRowNumber(context),
            .Rank => try self.executeRank(context),
            .DenseRank => try self.executeDenseRank(context),
            .PercentRank => try self.executePercentRank(context),
            .CumeDist => try self.executeCumeDist(context),
            .Ntile => try self.executeNtile(function, context),
            .Lead => try self.executeLead(function, context),
            .Lag => try self.executeLag(function, context),
            .FirstValue => try self.executeFirstValue(function, context),
            .LastValue => try self.executeLastValue(function, context),
            .NthValue => try self.executeNthValue(function, context),
        };
    }

    /// Execute ROW_NUMBER() window function
    fn executeRowNumber(self: *WindowExecutor, context: *WindowContext) !storage.Value {
        _ = self;
        const row_number = context.current_row - context.partition_start + 1;
        return storage.Value{ .Integer = @intCast(row_number) };
    }

    /// Execute RANK() window function
    fn executeRank(self: *WindowExecutor, context: *WindowContext) !storage.Value {
        _ = self;
        // For basic implementation, assume no ORDER BY, so all ranks are 1
        // TODO: Implement proper ranking based on ORDER BY clause
        const rank = context.current_row - context.partition_start + 1;
        return storage.Value{ .Integer = @intCast(rank) };
    }

    /// Execute DENSE_RANK() window function
    fn executeDenseRank(self: *WindowExecutor, context: *WindowContext) !storage.Value {
        _ = self;
        // For basic implementation, same as RANK
        // TODO: Implement proper dense ranking based on ORDER BY clause
        const dense_rank = context.current_row - context.partition_start + 1;
        return storage.Value{ .Integer = @intCast(dense_rank) };
    }

    /// Execute LEAD() window function
    fn executeLead(self: *WindowExecutor, function: ast.WindowFunction, context: *WindowContext) !storage.Value {
        _ = self;
        _ = function;

        const offset = 1; // Default lead offset
        const lead_row_index = context.current_row + offset;

        if (lead_row_index >= context.partition_end) {
            return storage.Value.Null; // No lead value available
        }

        // For basic implementation, return the first column of the lead row
        // TODO: Implement proper column selection based on function arguments
        if (context.rows[lead_row_index].values.len > 0) {
            return context.rows[lead_row_index].values[0];
        }

        return storage.Value.Null;
    }

    /// Execute LAG() window function
    fn executeLag(self: *WindowExecutor, function: ast.WindowFunction, context: *WindowContext) !storage.Value {
        _ = self;
        _ = function;

        const offset = 1; // Default lag offset

        if (context.current_row < context.partition_start + offset) {
            return storage.Value.Null; // No lag value available
        }

        const lag_row_index = context.current_row - offset;

        // For basic implementation, return the first column of the lag row
        // TODO: Implement proper column selection based on function arguments
        if (context.rows[lag_row_index].values.len > 0) {
            return context.rows[lag_row_index].values[0];
        }

        return storage.Value.Null;
    }

    /// Apply window functions to a result set
    pub fn applyWindowFunctions(
        self: *WindowExecutor,
        rows: *std.ArrayList(storage.Row),
        window_functions: []ast.WindowFunction,
    ) !void {
        if (window_functions.len == 0) return;

        var context = WindowContext.init(self.allocator, rows.items);

        // Process each row
        for (rows.items, 0..) |*row, i| {
            context.current_row = i;

            // Apply each window function
            for (window_functions) |window_func| {
                const result = try self.executeWindowFunction(window_func, &context);

                // Add the window function result as a new column
                var new_values = try self.allocator.alloc(storage.Value, row.values.len + 1);

                // Copy existing values
                for (row.values, 0..) |value, j| {
                    new_values[j] = value;
                }

                // Add window function result
                new_values[row.values.len] = result;

                // Replace the row's values
                self.allocator.free(row.values);
                row.values = new_values;
            }
        }
    }

    /// Execute PERCENT_RANK() window function
    fn executePercentRank(self: *WindowExecutor, context: *WindowContext) !storage.Value {
        _ = self;
        const partition_size = context.partition_end - context.partition_start;
        if (partition_size <= 1) return storage.Value{ .Real = 0.0 };

        const position = context.current_row - context.partition_start;
        const percent_rank = @as(f64, @floatFromInt(position)) / @as(f64, @floatFromInt(partition_size - 1));
        return storage.Value{ .Real = percent_rank };
    }

    /// Execute CUME_DIST() window function
    fn executeCumeDist(self: *WindowExecutor, context: *WindowContext) !storage.Value {
        _ = self;
        const partition_size = context.partition_end - context.partition_start;
        const position = context.current_row - context.partition_start + 1;
        const cume_dist = @as(f64, @floatFromInt(position)) / @as(f64, @floatFromInt(partition_size));
        return storage.Value{ .Real = cume_dist };
    }

    /// Execute NTILE() window function
    fn executeNtile(self: *WindowExecutor, function: ast.WindowFunction, context: *WindowContext) !storage.Value {
        _ = self;
        _ = function;
        const partition_size = context.partition_end - context.partition_start;
        const position = context.current_row - context.partition_start;

        // Default to 4 buckets if no argument specified
        const buckets = 4;
        const bucket_size = partition_size / buckets;
        const bucket = @min(position / bucket_size + 1, buckets);
        return storage.Value{ .Integer = @intCast(bucket) };
    }

    /// Execute FIRST_VALUE() window function
    fn executeFirstValue(self: *WindowExecutor, function: ast.WindowFunction, context: *WindowContext) !storage.Value {
        _ = self;
        _ = function;
        if (context.partition_start < context.rows.len) {
            // Return the first value in the partition (stub - would need expression evaluation)
            return storage.Value{ .Null = {} };
        }
        return storage.Value{ .Null = {} };
    }

    /// Execute LAST_VALUE() window function
    fn executeLastValue(self: *WindowExecutor, function: ast.WindowFunction, context: *WindowContext) !storage.Value {
        _ = self;
        _ = function;
        if (context.partition_end > 0) {
            // Return the last value in the partition (stub - would need expression evaluation)
            return storage.Value{ .Null = {} };
        }
        return storage.Value{ .Null = {} };
    }

    /// Execute NTH_VALUE() window function
    fn executeNthValue(self: *WindowExecutor, function: ast.WindowFunction, context: *WindowContext) !storage.Value {
        _ = self;
        _ = function;
        _ = context;
        // Stub implementation - would need to extract N from function arguments
        return storage.Value{ .Null = {} };
    }
};

/// Window function type enumeration
pub const WindowFunctionType = enum {
    RowNumber,
    Rank,
    DenseRank,
    Lead,
    Lag,
};

// Tests
test "window function ROW_NUMBER" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var executor = WindowExecutor.init(allocator);

    // Create test data
    var rows = std.ArrayList(storage.Row).init(allocator);
    defer {
        for (rows.items) |row| {
            allocator.free(row.values);
        }
        rows.deinit();
    }

    // Add test rows
    for (0..5) |i| {
        var values = try allocator.alloc(storage.Value, 1);
        values[0] = storage.Value{ .Integer = @intCast(i) };
        try rows.append(storage.Row{ .values = values });
    }

    // Create window function
    const window_func = ast.WindowFunction{
        .function_type = .RowNumber,
        .arguments = &[_]ast.FunctionArgument{},
        .window_spec = ast.WindowSpecification{
            .partition_by = &[_]ast.Expression{},
            .order_by = &[_]ast.OrderByClause{},
            .frame = null,
        },
    };

    // Apply window function
    try executor.applyWindowFunctions(&rows, &[_]ast.WindowFunction{window_func});

    // Verify results
    for (rows.items, 0..) |row, i| {
        try std.testing.expect(row.values.len == 2); // Original + ROW_NUMBER
        try std.testing.expect(row.values[1].Integer == @as(i32, @intCast(i + 1)));
    }
}

test "window function context" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create test rows
    var values1 = try allocator.alloc(storage.Value, 1);
    values1[0] = storage.Value{ .Integer = 1 };
    var values2 = try allocator.alloc(storage.Value, 1);
    values2[0] = storage.Value{ .Integer = 2 };

    const rows = [_]storage.Row{
        storage.Row{ .values = values1 },
        storage.Row{ .values = values2 },
    };

    const context = WindowContext.init(allocator, @constCast(&rows));

    try std.testing.expect(context.rows.len == 2);
    try std.testing.expect(context.partition_start == 0);
    try std.testing.expect(context.partition_end == 2);

    allocator.free(values1);
    allocator.free(values2);
}
