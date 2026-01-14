const std = @import("std");
const ast = @import("../parser/ast.zig");
const planner = @import("planner.zig");
const storage = @import("../db/storage.zig");
const db = @import("../db/connection.zig");
const functions = @import("functions.zig");

/// Virtual machine for executing query plans
pub const VirtualMachine = struct {
    connection: *db.Connection,
    parameters: ?[]storage.Value, // Optional parameters for prepared statements
    function_evaluator: functions.FunctionEvaluator,
    /// CTE context: maps CTE names to their result rows
    cte_context: std.StringHashMap(CTEResult),
    /// Current table being scanned (for column name resolution in projection)
    current_table: ?*storage.Table,

    const Self = @This();

    /// Error set for condition/expression evaluation
    pub const EvalError = error{
        OutOfMemory,
        ColumnNotFound,
        DivisionByZero,
        TypeMismatch,
        InvalidOperator,
        ParameterNotBound,
        InvalidFunctionCall,
        FunctionNotFound,
        InvalidArgumentCount,
        Overflow,
    };

    /// Stored CTE result
    pub const CTEResult = struct {
        rows: []storage.Row,
        column_names: ?[][]const u8,

        pub fn deinit(self: *CTEResult, allocator: std.mem.Allocator) void {
            for (self.rows) |row| {
                for (row.values) |value| {
                    value.deinit(allocator);
                }
                allocator.free(row.values);
            }
            allocator.free(self.rows);

            if (self.column_names) |cols| {
                for (cols) |col| {
                    allocator.free(col);
                }
                allocator.free(cols);
            }
        }
    };

    /// Initialize virtual machine
    pub fn init(allocator: std.mem.Allocator, connection: *db.Connection) Self {
        // Always use the connection's allocator to ensure consistency
        _ = allocator; // Ignore passed allocator, use connection's allocator
        // VM initialization complete
        return Self{
            .connection = connection,
            .parameters = null,
            .function_evaluator = functions.FunctionEvaluator.init(connection.allocator),
            .cte_context = std.StringHashMap(CTEResult).init(connection.allocator),
            .current_table = null,
        };
    }

    /// Clean up VM resources including CTE context
    pub fn deinitVM(self: *Self) void {
        self.clearCTEContext();
        self.cte_context.deinit();
    }

    /// Clear all CTE results
    fn clearCTEContext(self: *Self) void {
        var iter = self.cte_context.iterator();
        while (iter.next()) |entry| {
            var result = entry.value_ptr.*;
            result.deinit(self.connection.allocator);
        }
        self.cte_context.clearRetainingCapacity();
    }

    /// Execute a query plan
    pub fn execute(self: *Self, plan: *planner.ExecutionPlan) !ExecutionResult {
        var result = ExecutionResult{
            .rows = .{},
            .affected_rows = 0,
            .connection = self.connection,
        };

        for (plan.steps) |*step| {
            try self.executeStep(step, &result);
        }

        // Execution completed
        return result;
    }

    /// Execute a query plan with parameters (for prepared statements)
    pub fn executeWithParameters(self: *Self, plan: *planner.ExecutionPlan, parameters: []storage.Value) !ExecutionResult {
        // Set parameters for this execution
        self.parameters = parameters;
        defer self.parameters = null; // Clear parameters after execution

        return self.execute(plan);
    }

    /// Execute a single step
    fn executeStep(self: *Self, step: *planner.ExecutionStep, result: *ExecutionResult) !void {
        switch (step.*) {
            .TableScan => |*scan| try self.executeTableScan(scan, result),
            .Filter => |*filter| try self.executeFilter(filter, result),
            .Project => |*project| try self.executeProject(project, result),
            .Limit => |*limit| try self.executeLimit(limit, result),
            .Insert => |*insert| try self.executeInsert(insert, result),
            .CreateTable => |*create| try self.executeCreateTable(create, result),
            .Update => |*update| try self.executeUpdate(update, result),
            .Delete => |*delete| try self.executeDelete(delete, result),
            .NestedLoopJoin => |*join| try self.executeNestedLoopJoin(join, result),
            .HashJoin => |*join| try self.executeHashJoin(join, result),
            .Aggregate => |*agg| try self.executeAggregate(agg, result),
            .GroupBy => |*group| try self.executeGroupBy(group, result),
            .BeginTransaction => try self.executeBeginTransaction(result),
            .Commit => try self.executeCommit(result),
            .Rollback => try self.executeRollback(result),
            .CreateIndex => |*create_idx| try self.executeCreateIndex(create_idx, result),
            .DropIndex => |*drop_idx| try self.executeDropIndex(drop_idx, result),
            .DropTable => |*drop_tbl| try self.executeDropTable(drop_tbl, result),
            .CreateCTE => |*cte| try self.executeCreateCTE(cte, result),
            .Pragma => |*pragma| try self.executePragma(pragma, result),
            .Explain => |*explain| try self.executeExplain(explain, result),
        }
    }

    /// Execute CTE creation - stores the CTE result for later reference
    fn executeCreateCTE(self: *Self, cte: *planner.CreateCTEStep, result: *ExecutionResult) anyerror!void {
        // Create a temporary result to execute the CTE subquery
        var cte_result = ExecutionResult{
            .rows = .{},
            .affected_rows = 0,
            .connection = self.connection,
        };

        // Execute the CTE's subquery steps (non-recursive since CTEs can't contain CTEs in subquery)
        for (cte.subquery_steps) |*step| {
            try self.executeNonCTEStep(step, &cte_result);
        }

        // Clone the result rows for storage in CTE context
        var stored_rows = try self.connection.allocator.alloc(storage.Row, cte_result.rows.items.len);
        for (cte_result.rows.items, 0..) |row, i| {
            var cloned_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
            for (row.values, 0..) |value, j| {
                cloned_values[j] = try value.clone(self.connection.allocator);
            }
            stored_rows[i] = storage.Row{ .values = cloned_values };
        }

        // Clone column names if provided
        var stored_column_names: ?[][]const u8 = null;
        if (cte.column_names) |cols| {
            var cloned_cols = try self.connection.allocator.alloc([]const u8, cols.len);
            for (cols, 0..) |col, i| {
                cloned_cols[i] = try self.connection.allocator.dupe(u8, col);
            }
            stored_column_names = cloned_cols;
        }

        // Store in CTE context
        try self.cte_context.put(cte.name, CTEResult{
            .rows = stored_rows,
            .column_names = stored_column_names,
        });

        // Clean up temporary result
        cte_result.deinit();

        // CTE creation doesn't affect the main result
        _ = result;
    }

    /// Execute a step that is not a CTE (used by CTE execution to avoid recursion)
    fn executeNonCTEStep(self: *Self, step: *planner.ExecutionStep, result: *ExecutionResult) !void {
        switch (step.*) {
            .TableScan => |*scan| try self.executeTableScan(scan, result),
            .Filter => |*filter| try self.executeFilter(filter, result),
            .Project => |*project| try self.executeProject(project, result),
            .Limit => |*limit| try self.executeLimit(limit, result),
            .Insert => |*insert| try self.executeInsert(insert, result),
            .CreateTable => |*create| try self.executeCreateTable(create, result),
            .Update => |*update| try self.executeUpdate(update, result),
            .Delete => |*delete| try self.executeDelete(delete, result),
            .NestedLoopJoin => |*join| try self.executeNestedLoopJoin(join, result),
            .HashJoin => |*join| try self.executeHashJoin(join, result),
            .Aggregate => |*agg| try self.executeAggregate(agg, result),
            .GroupBy => |*group| try self.executeGroupBy(group, result),
            .BeginTransaction => try self.executeBeginTransaction(result),
            .Commit => try self.executeCommit(result),
            .Rollback => try self.executeRollback(result),
            .CreateIndex => |*create_idx| try self.executeCreateIndex(create_idx, result),
            .DropIndex => |*drop_idx| try self.executeDropIndex(drop_idx, result),
            .DropTable => |*drop_tbl| try self.executeDropTable(drop_tbl, result),
            .CreateCTE => {
                // CTEs within CTEs are not supported in this version
                return error.NestedCTENotSupported;
            },
            .Pragma => |*pragma| try self.executePragma(pragma, result),
            .Explain => |*explain| try self.executeExplain(explain, result),
        }
    }

    /// Execute table scan
    fn executeTableScan(self: *Self, scan: *planner.TableScanStep, result: *ExecutionResult) !void {
        // First check if this is a CTE reference
        if (self.cte_context.get(scan.table_name)) |cte_result| {
            // Use CTE results instead of table
            for (cte_result.rows) |row| {
                // Clone the row for the result
                var cloned_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
                for (row.values, 0..) |value, j| {
                    cloned_values[j] = try value.clone(self.connection.allocator);
                }
                try result.rows.append(self.connection.allocator, storage.Row{ .values = cloned_values });
            }
            return;
        }

        // Executing table scan on actual table
        const table = self.connection.storage_engine.getTable(scan.table_name) orelse {
            return error.TableNotFound;
        };

        // Track the current table for column name resolution in projection
        self.current_table = table;

        const rows = try table.select(self.connection.allocator);
        defer {
            // Only free the rows array itself - the individual rows are now owned by result
            self.connection.allocator.free(rows);
        }

        for (rows) |row| {
            // The row is already properly cloned by btree.selectAll - use it directly
            // Using pre-cloned row from btree
            for (row.values) |value| {
                switch (value) {
                    .Text => {
                        // Using pre-cloned text value
                    },
                    else => {},
                }
            }
            try result.rows.append(self.connection.allocator, row);
            // Row appended to result
        }
    }

    /// Execute filter (WHERE clause)
    fn executeFilter(self: *Self, filter: *planner.FilterStep, result: *ExecutionResult) !void {
        var filtered_rows: std.ArrayList(storage.Row) = .{};

        for (result.rows.items) |row| {
            if (try self.evaluateCondition(&filter.condition, &row)) {
                try filtered_rows.append(self.connection.allocator, row);
            } else {
                // Free rows that don't match the filter condition
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
        }

        // Only free the ArrayList structure, not its contents (they're now in filtered_rows or freed)
        result.rows.deinit(self.connection.allocator);
        result.rows = filtered_rows;
    }

    /// Execute projection (SELECT columns)
    fn executeProject(self: *Self, project: *planner.ProjectStep, result: *ExecutionResult) !void {
        if (project.columns.len == 1 and std.mem.eql(u8, project.columns[0], "*")) {
            // SELECT * - return all columns, no projection needed
            return;
        }

        // Use the current table schema to map column names to indices
        const table = self.current_table;

        // Create projected rows with only selected columns
        var projected_rows: std.ArrayList(storage.Row) = .{};

        // Track which original indices we're using to properly handle memory
        const num_cols = if (table) |t| t.schema.columns.len else project.columns.len;
        var used_indices = try self.connection.allocator.alloc(bool, num_cols);
        defer self.connection.allocator.free(used_indices);

        for (result.rows.items) |original_row| {
            @memset(used_indices, false);
            var projected_values: std.ArrayList(storage.Value) = .{};

            for (project.columns, 0..) |col_name, col_i| {
                // Check if we have an expression for this column
                if (project.expressions) |exprs| {
                    const expr = &exprs[col_i];
                    switch (expr.*) {
                        .Case => |case_expr| {
                            // Evaluate CASE expression
                            const case_value = try self.evaluateCaseExpression(&case_expr, &original_row);
                            try projected_values.append(self.connection.allocator, case_value);
                            continue;
                        },
                        .FunctionCall => |func_call| {
                            // Evaluate function call with row context (COALESCE, NULLIF, IFNULL, etc.)
                            const func_value = try self.evaluateFunctionWithRow(func_call, &original_row);
                            try projected_values.append(self.connection.allocator, func_value);
                            continue;
                        },
                        .Simple => {
                            // Fall through to normal column lookup
                        },
                        else => {
                            // Other expressions not yet supported (Window, Aggregate in non-aggregate context)
                            try projected_values.append(self.connection.allocator, storage.Value.Null);
                            continue;
                        },
                    }
                }

                // Find the column index by name in the table schema
                var col_idx: ?usize = null;
                if (table) |t| {
                    for (t.schema.columns, 0..) |col, idx| {
                        if (std.mem.eql(u8, col.name, col_name)) {
                            col_idx = idx;
                            break;
                        }
                    }
                }

                if (col_idx) |idx| {
                    if (idx < original_row.values.len) {
                        // Transfer ownership of the value
                        try projected_values.append(self.connection.allocator, original_row.values[idx]);
                        used_indices[idx] = true;
                    } else {
                        try projected_values.append(self.connection.allocator, storage.Value.Null);
                    }
                } else {
                    // Column not found, add NULL
                    try projected_values.append(self.connection.allocator, storage.Value.Null);
                }
            }

            try projected_rows.append(self.connection.allocator, storage.Row{
                .values = try projected_values.toOwnedSlice(self.connection.allocator),
            });

            // Free values that weren't used in projection
            for (original_row.values, 0..) |value, idx| {
                if (!used_indices[idx]) {
                    value.deinit(self.connection.allocator);
                }
            }
            self.connection.allocator.free(original_row.values);
        }

        result.rows.deinit(self.connection.allocator);
        result.rows = projected_rows;
    }

    /// Resolve a value, substituting parameters if needed
    /// IMPORTANT: The returned value is cloned and must be freed by the caller
    fn resolveValue(self: *Self, value: storage.Value) !storage.Value {
        return switch (value) {
            .Parameter => |param_index| blk: {
                if (self.parameters) |params| {
                    if (param_index < params.len) {
                        // Clone the parameter value so caller can safely free it
                        break :blk try self.cloneValue(params[param_index]);
                    } else {
                        return error.ParameterIndexOutOfBounds;
                    }
                } else {
                    return error.NoParametersProvided;
                }
            },
            .FunctionCall => |function_call| blk: {
                // Evaluate function call and return the result
                const ast_function_call = try self.convertStorageFunctionToAst(function_call);
                defer ast_function_call.deinit(self.connection.allocator);

                break :blk try self.function_evaluator.evaluateFunction(ast_function_call);
            },
            // For other value types, clone if they have heap allocations
            .Text => |t| storage.Value{ .Text = try self.connection.allocator.dupe(u8, t) },
            .Blob => |b| storage.Value{ .Blob = try self.connection.allocator.dupe(u8, b) },
            .JSON => |j| storage.Value{ .JSON = try self.connection.allocator.dupe(u8, j) },
            else => value, // Integer, Real, Null, etc. don't need cloning
        };
    }

    /// Evaluate a default value, including function calls
    fn evaluateDefaultValue(self: *Self, default_value: ast.DefaultValue) !storage.Value {
        return switch (default_value) {
            .Literal => |literal| {
                const storage_value = try self.convertAstValueToStorage(literal);
                return self.resolveValue(storage_value);
            },
            .FunctionCall => |function_call| {
                return self.function_evaluator.evaluateFunction(function_call);
            },
        };
    }

    /// Convert AST value to storage value
    fn convertAstValueToStorage(self: *Self, value: ast.Value) !storage.Value {
        return switch (value) {
            .Integer => |i| storage.Value{ .Integer = i },
            .Text => |t| storage.Value{ .Text = try self.connection.allocator.dupe(u8, t) },
            .Real => |r| storage.Value{ .Real = r },
            .Blob => |b| storage.Value{ .Blob = try self.connection.allocator.dupe(u8, b) },
            .Null => storage.Value.Null,
            .Parameter => |param_index| storage.Value{ .Parameter = param_index },
            .FunctionCall => |function_call| {
                const storage_func = try self.convertAstFunctionToStorage(function_call);
                return storage.Value{ .FunctionCall = storage_func };
            },
        };
    }

    /// Evaluate a storage default value
    fn evaluateStorageDefaultValue(self: *Self, default_value: storage.Column.DefaultValue) !storage.Value {
        return switch (default_value) {
            .Literal => |literal| {
                // resolveValue already clones heap-allocated values (Text, Blob, JSON)
                // so we don't need to clone again
                return try self.resolveValue(literal);
            },
            .FunctionCall => |function_call| {
                // Convert storage function call to AST function call for evaluation
                const ast_function_call = try self.convertStorageFunctionToAst(function_call);
                defer ast_function_call.deinit(self.connection.allocator);

                return self.function_evaluator.evaluateFunction(ast_function_call);
            },
        };
    }

    /// Convert AST function call to storage function call
    fn convertAstFunctionToStorage(self: *Self, function_call: ast.FunctionCall) !storage.Column.FunctionCall {
        var storage_args = try self.connection.allocator.alloc(storage.Column.FunctionArgument, function_call.arguments.len);
        for (function_call.arguments, 0..) |arg, i| {
            storage_args[i] = try self.convertAstFunctionArgToStorage(arg);
        }

        return storage.Column.FunctionCall{
            .name = try self.connection.allocator.dupe(u8, function_call.name),
            .arguments = storage_args,
        };
    }

    /// Convert AST function argument to storage function argument
    fn convertAstFunctionArgToStorage(self: *Self, arg: ast.FunctionArgument) !storage.Column.FunctionArgument {
        return switch (arg) {
            .Literal => |literal| {
                const storage_value = try self.convertAstValueToStorage(literal);
                return storage.Column.FunctionArgument{ .Literal = storage_value };
            },
            .String => |string| {
                const text_value = storage.Value{ .Text = try self.connection.allocator.dupe(u8, string) };
                return storage.Column.FunctionArgument{ .Literal = text_value };
            },
            .Column => |column| {
                return storage.Column.FunctionArgument{ .Column = try self.connection.allocator.dupe(u8, column) };
            },
            .Parameter => |param_index| {
                return storage.Column.FunctionArgument{ .Parameter = param_index };
            },
        };
    }

    /// Convert storage function call to AST function call
    fn convertStorageFunctionToAst(self: *Self, function_call: storage.Column.FunctionCall) anyerror!ast.FunctionCall {
        var ast_args = try self.connection.allocator.alloc(ast.FunctionArgument, function_call.arguments.len);
        for (function_call.arguments, 0..) |arg, i| {
            ast_args[i] = try self.convertStorageFunctionArgToAst(arg);
        }

        return ast.FunctionCall{
            .name = try self.connection.allocator.dupe(u8, function_call.name),
            .arguments = ast_args,
        };
    }

    /// Convert storage function argument to AST function argument
    fn convertStorageFunctionArgToAst(self: *Self, arg: storage.Column.FunctionArgument) anyerror!ast.FunctionArgument {
        return switch (arg) {
            .Literal => |literal| {
                const ast_value = try self.convertStorageValueToAst(literal);
                return ast.FunctionArgument{ .Literal = ast_value };
            },
            .Column => |column| {
                return ast.FunctionArgument{ .Column = try self.connection.allocator.dupe(u8, column) };
            },
            .Parameter => |param_index| {
                return ast.FunctionArgument{ .Parameter = param_index };
            },
        };
    }

    /// Convert storage value to AST value
    fn convertStorageValueToAst(self: *Self, value: storage.Value) anyerror!ast.Value {
        return switch (value) {
            .Integer => |i| ast.Value{ .Integer = i },
            .Text => |t| ast.Value{ .Text = try self.connection.allocator.dupe(u8, t) },
            .Real => |r| ast.Value{ .Real = r },
            .Blob => |b| ast.Value{ .Blob = try self.connection.allocator.dupe(u8, b) },
            .Null => ast.Value.Null,
            .Parameter => |param_index| ast.Value{ .Parameter = param_index },
            .FunctionCall => |function_call| {
                const ast_function_call = try self.convertStorageFunctionToAst(function_call);
                return ast.Value{ .FunctionCall = ast_function_call };
            },
            // PostgreSQL compatibility values
            .JSON => |j| ast.Value{ .Text = try self.connection.allocator.dupe(u8, j) },
            .JSONB => |jsonb| ast.Value{ .Text = try jsonb.toString(self.connection.allocator) },
            .UUID => |uuid| ast.Value{ .Text = try ast.UUIDUtils.toString(uuid, self.connection.allocator) },
            .Array => |array| ast.Value{ .Text = try array.toString(self.connection.allocator) },
            .Boolean => |b| ast.Value{ .Integer = if (b) 1 else 0 },
            .Timestamp => |ts| ast.Value{ .Integer = @intCast(ts) },
            .TimestampTZ => |tstz| ast.Value{ .Text = try std.fmt.allocPrint(self.connection.allocator, "{}", .{tstz.timestamp}) },
            .Date => |d| ast.Value{ .Integer = d },
            .Time => |t| ast.Value{ .Integer = @intCast(t) },
            .Interval => |i| ast.Value{ .Integer = @intCast(i) },
            .Numeric => |n| ast.Value{ .Text = try std.fmt.allocPrint(self.connection.allocator, "NUMERIC({},{})", .{ n.precision, n.scale }) },
            .SmallInt => |si| ast.Value{ .Integer = si },
            .BigInt => |bi| ast.Value{ .Integer = @intCast(bi) },
        };
    }

    /// Clone a storage default value
    fn cloneStorageDefaultValue(self: *Self, default_value: storage.Column.DefaultValue) !storage.Column.DefaultValue {
        return switch (default_value) {
            .Literal => |literal| {
                // Create a deep clone to avoid double-free issues
                const cloned_literal = try literal.clone(self.connection.allocator);
                return storage.Column.DefaultValue{ .Literal = cloned_literal };
            },
            .FunctionCall => |function_call| {
                // For function calls, create a proper deep clone
                const cloned_function_call = try self.cloneStorageFunctionCallDeep(function_call);
                return storage.Column.DefaultValue{ .FunctionCall = cloned_function_call };
            },
        };
    }

    /// Clone a storage function call (shallow - for compatibility)
    fn cloneStorageFunctionCall(self: *Self, function_call: storage.Column.FunctionCall) anyerror!storage.Column.FunctionCall {
        return self.cloneStorageFunctionCallDeep(function_call);
    }

    /// Clone a storage function call with deep cloning to prevent double-free
    fn cloneStorageFunctionCallDeep(self: *Self, function_call: storage.Column.FunctionCall) anyerror!storage.Column.FunctionCall {
        var cloned_args = try self.connection.allocator.alloc(storage.Column.FunctionArgument, function_call.arguments.len);
        errdefer self.connection.allocator.free(cloned_args);

        var args_cloned: usize = 0;
        errdefer {
            for (cloned_args[0..args_cloned]) |arg| {
                self.deallocateStorageFunctionArgument(arg);
            }
        }

        for (function_call.arguments, 0..) |arg, i| {
            cloned_args[i] = try self.cloneStorageFunctionArgumentDeep(arg);
            args_cloned = i + 1;
        }

        return storage.Column.FunctionCall{
            .name = try self.connection.allocator.dupe(u8, function_call.name),
            .arguments = cloned_args,
        };
    }

    /// Clone a storage function argument
    fn cloneStorageFunctionArgument(self: *Self, arg: storage.Column.FunctionArgument) anyerror!storage.Column.FunctionArgument {
        return self.cloneStorageFunctionArgumentDeep(arg);
    }

    /// Clone a storage function argument with deep cloning
    fn cloneStorageFunctionArgumentDeep(self: *Self, arg: storage.Column.FunctionArgument) anyerror!storage.Column.FunctionArgument {
        return switch (arg) {
            .Literal => |literal| {
                const cloned_literal = try literal.clone(self.connection.allocator);
                return storage.Column.FunctionArgument{ .Literal = cloned_literal };
            },
            .Column => |column| {
                return storage.Column.FunctionArgument{ .Column = try self.connection.allocator.dupe(u8, column) };
            },
            .Parameter => |param_index| {
                return storage.Column.FunctionArgument{ .Parameter = param_index };
            },
        };
    }

    /// Deallocate a storage function argument
    fn deallocateStorageFunctionArgument(self: *Self, arg: storage.Column.FunctionArgument) void {
        switch (arg) {
            .Literal => |literal| literal.deinit(self.connection.allocator),
            .Column => |column| self.connection.allocator.free(column),
            .Parameter => {}, // No deallocation needed
        }
    }

    fn cloneValue(self: *Self, value: storage.Value) !storage.Value {
        // Note: cloneValue called from non-table-scan operation
        const cloned = try value.clone(self.connection.allocator);
        switch (value) {
            .Text => {
                switch (cloned) {
                    .Text => {
                        // Cloning text value in non-table-scan operation
                    },
                    else => unreachable,
                }
            },
            else => {},
        }
        return cloned;
    }

    /// Execute limit
    fn executeLimit(self: *Self, limit: *planner.LimitStep, result: *ExecutionResult) !void {
        const start = @min(limit.offset, result.rows.items.len);
        const end = @min(start + limit.count, result.rows.items.len);

        if (start > 0 or end < result.rows.items.len) {
            // Free rows excluded by OFFSET (rows before start)
            for (result.rows.items[0..start]) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }

            // Free rows excluded by LIMIT (rows after end)
            for (result.rows.items[end..]) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }

            // Create new slice with limited rows
            var limited_rows: std.ArrayList(storage.Row) = .{};
            for (result.rows.items[start..end]) |row| {
                try limited_rows.append(self.connection.allocator, row);
            }
            result.rows.deinit(self.connection.allocator);
            result.rows = limited_rows;
        }
    }

    /// Execute insert
    fn executeInsert(self: *Self, insert: *planner.InsertStep, result: *ExecutionResult) !void {
        const table = self.connection.storage_engine.getTable(insert.table_name) orelse {
            return error.TableNotFound;
        };

        for (insert.values) |row_values| {
            // Build final values array for all table columns
            var final_values = try self.connection.allocator.alloc(storage.Value, table.schema.columns.len);
            var values_initialized: usize = 0;
            errdefer {
                for (final_values[0..values_initialized]) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(final_values);
            }

            // Initialize all values to null first
            for (final_values) |*value| {
                value.* = storage.Value.Null;
            }

            if (insert.columns) |specified_columns| {
                // INSERT with specific columns: INSERT INTO table (col1, col2) VALUES (...)
                if (specified_columns.len != row_values.len) {
                    return error.ColumnValueMismatch;
                }

                // Map provided values to specified columns
                for (specified_columns, 0..) |col_name, value_idx| {
                    // Find the column index in the table schema
                    var table_col_idx: ?usize = null;
                    for (table.schema.columns, 0..) |table_col, table_idx| {
                        if (std.mem.eql(u8, table_col.name, col_name)) {
                            table_col_idx = table_idx;
                            break;
                        }
                    }

                    if (table_col_idx == null) {
                        return error.ColumnNotFound;
                    }

                    // resolveValue already returns owned/cloned values
                    const resolved_value = try self.resolveValue(row_values[value_idx]);
                    final_values[table_col_idx.?] = resolved_value;
                    values_initialized = @max(values_initialized, table_col_idx.? + 1);
                }
            } else {
                // INSERT without column specification: INSERT INTO table VALUES (...)
                // Values are provided in table column order
                for (row_values, 0..) |value, i| {
                    if (i >= table.schema.columns.len) {
                        return error.TooManyValues;
                    }
                    // resolveValue already returns owned/cloned values
                    const resolved_value = try self.resolveValue(value);
                    final_values[i] = resolved_value;
                    values_initialized = i + 1;
                }
            }

            // Apply default values for columns that weren't specified
            for (table.schema.columns, 0..) |column, i| {
                if (final_values[i] == .Null) {
                    if (column.default_value) |default_value| {
                        // Replace NULL with evaluated default value
                        const default_val = try self.evaluateStorageDefaultValue(default_value);
                        final_values[i] = default_val;
                        values_initialized = @max(values_initialized, i + 1);
                    } else if (!column.is_nullable) {
                        // Non-nullable column without default value
                        return error.MissingRequiredValue;
                    }
                    // For nullable columns without defaults, keep as NULL
                }
            }

            const row = storage.Row{ .values = final_values };
            try table.insert(row);
            result.affected_rows += 1;
        }
    }

    /// Execute create table
    fn executeCreateTable(self: *Self, create: *planner.CreateTableStep, result: *ExecutionResult) !void {
        // Check if table exists and if_not_exists is true
        if (create.if_not_exists and self.connection.storage_engine.getTable(create.table_name) != null) {
            return; // Table already exists, skip creation
        }

        // Clone columns for the storage engine (they're owned by the planner otherwise)
        var cloned_columns = try self.connection.allocator.alloc(storage.Column, create.columns.len);

        for (create.columns, 0..) |column, i| {
            cloned_columns[i] = storage.Column{
                .name = self.connection.allocator.dupe(u8, column.name) catch {
                    // Clean up already cloned columns on failure
                    for (cloned_columns[0..i]) |c| {
                        self.connection.allocator.free(c.name);
                        if (c.default_value) |dv| dv.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(cloned_columns);
                    return error.OutOfMemory;
                },
                .data_type = column.data_type,
                .is_primary_key = column.is_primary_key,
                .is_nullable = column.is_nullable,
                .default_value = if (column.default_value) |default_value|
                    self.cloneStorageDefaultValue(default_value) catch {
                        self.connection.allocator.free(cloned_columns[i].name);
                        for (cloned_columns[0..i]) |c| {
                            self.connection.allocator.free(c.name);
                            if (c.default_value) |dv| dv.deinit(self.connection.allocator);
                        }
                        self.connection.allocator.free(cloned_columns);
                        return error.OutOfMemory;
                    }
                else
                    null,
            };
        }

        var schema = storage.TableSchema{
            .columns = cloned_columns,
        };
        defer schema.deinit(self.connection.allocator);

        try self.connection.storage_engine.createTable(create.table_name, schema);
        result.affected_rows = 1;
    }

    /// Execute update
    fn executeUpdate(self: *Self, update: *planner.UpdateStep, result: *ExecutionResult) !void {
        const table = self.connection.storage_engine.getTable(update.table_name) orelse {
            return error.TableNotFound;
        };

        // Track the current table for column name resolution in condition evaluation
        self.current_table = table;

        // Get all current rows
        const all_rows = try table.select(self.connection.allocator);
        defer {
            for (all_rows) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            self.connection.allocator.free(all_rows);
        }

        var updated_count: u32 = 0;
        var updated_rows: std.ArrayList(storage.Row) = .{};
        defer {
            for (updated_rows.items) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            updated_rows.deinit(self.connection.allocator);
        }

        for (all_rows) |row| {
            // Check if row matches condition
            var matches = true;
            if (update.condition) |condition| {
                matches = try self.evaluateCondition(&condition, &row);
            }

            if (matches) {
                // Create updated row by cloning the original and applying changes
                var updated_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
                var values_cloned: usize = 0;
                errdefer {
                    for (updated_values[0..values_cloned]) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(updated_values);
                }

                for (row.values, 0..) |value, i| {
                    updated_values[i] = try self.cloneValue(value);
                    values_cloned = i + 1;
                }

                // Apply assignments - look up column by name to get correct index
                for (update.assignments) |assignment| {
                    var col_idx: ?usize = null;
                    for (table.schema.columns, 0..) |col, idx| {
                        if (std.mem.eql(u8, col.name, assignment.column)) {
                            col_idx = idx;
                            break;
                        }
                    }
                    if (col_idx) |idx| {
                        if (idx < updated_values.len) {
                            updated_values[idx].deinit(self.connection.allocator);
                            updated_values[idx] = try self.cloneValue(assignment.value);
                        }
                    }
                }

                try updated_rows.append(self.connection.allocator, storage.Row{ .values = updated_values });
                updated_count += 1;
            } else {
                // Keep the original row unchanged
                var cloned_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
                var values_cloned: usize = 0;
                errdefer {
                    for (cloned_values[0..values_cloned]) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(cloned_values);
                }

                for (row.values, 0..) |value, i| {
                    cloned_values[i] = try self.cloneValue(value);
                    values_cloned = i + 1;
                }
                try updated_rows.append(self.connection.allocator, storage.Row{ .values = cloned_values });
            }
        }

        // Replace the table data with updated rows
        // For now, we'll recreate the table (in a real implementation, we'd have proper update methods)
        const table_name = try self.connection.allocator.dupe(u8, update.table_name);
        defer self.connection.allocator.free(table_name);

        // Clone table schema before dropping the table to avoid use-after-free
        var cloned_columns = try self.connection.allocator.alloc(storage.Column, table.schema.columns.len);
        var columns_cloned: usize = 0;
        errdefer {
            // Clean up cloned columns on error
            for (cloned_columns[0..columns_cloned]) |column| {
                self.connection.allocator.free(column.name);
                if (column.default_value) |default_value| {
                    default_value.deinit(self.connection.allocator);
                }
            }
            self.connection.allocator.free(cloned_columns);
        }

        for (table.schema.columns, 0..) |column, i| {
            cloned_columns[i] = storage.Column{
                .name = try self.connection.allocator.dupe(u8, column.name),
                .data_type = column.data_type,
                .is_primary_key = column.is_primary_key,
                .is_nullable = column.is_nullable,
                .default_value = if (column.default_value) |default_value|
                    try self.cloneStorageDefaultValue(default_value)
                else
                    null,
            };
            columns_cloned = i + 1;
        }

        var cloned_schema = storage.TableSchema{
            .columns = cloned_columns,
        };

        // Drop and recreate table with updated data
        try self.connection.storage_engine.dropTable(update.table_name);
        try self.connection.storage_engine.createTable(table_name, cloned_schema);

        // Clean up temporary schema (storage engine has its own clone now)
        cloned_schema.deinit(self.connection.allocator);

        // Reinsert all rows
        const new_table = self.connection.storage_engine.getTable(update.table_name).?;
        for (updated_rows.items) |row| {
            // Clone the row for insertion
            var insert_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
            var values_cloned: usize = 0;
            errdefer {
                for (insert_values[0..values_cloned]) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(insert_values);
            }

            for (row.values, 0..) |value, i| {
                insert_values[i] = try self.cloneValue(value);
                values_cloned = i + 1;
            }
            try new_table.insert(storage.Row{ .values = insert_values });
        }

        result.affected_rows = updated_count;
    }

    /// Execute delete
    fn executeDelete(self: *Self, delete: *planner.DeleteStep, result: *ExecutionResult) !void {
        const table = self.connection.storage_engine.getTable(delete.table_name) orelse {
            return error.TableNotFound;
        };

        // Track the current table for column name resolution in condition evaluation
        self.current_table = table;

        // Get all current rows
        const all_rows = try table.select(self.connection.allocator);
        defer {
            for (all_rows) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            self.connection.allocator.free(all_rows);
        }

        var deleted_count: u32 = 0;
        var surviving_rows: std.ArrayList(storage.Row) = .{};
        defer {
            for (surviving_rows.items) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            surviving_rows.deinit(self.connection.allocator);
        }

        for (all_rows) |row| {
            // Check if row matches delete condition
            var should_delete = true;
            if (delete.condition) |condition| {
                should_delete = try self.evaluateCondition(&condition, &row);
            }

            if (should_delete) {
                deleted_count += 1;
            } else {
                // Keep this row - clone it for the surviving rows
                var cloned_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
                var values_cloned: usize = 0;
                errdefer {
                    for (cloned_values[0..values_cloned]) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(cloned_values);
                }

                for (row.values, 0..) |value, i| {
                    cloned_values[i] = try self.cloneValue(value);
                    values_cloned = i + 1;
                }
                try surviving_rows.append(self.connection.allocator, storage.Row{ .values = cloned_values });
            }
        }

        // Replace the table data with surviving rows
        if (deleted_count > 0) {
            const table_name = try self.connection.allocator.dupe(u8, delete.table_name);
            defer self.connection.allocator.free(table_name);

            // Clone table schema before dropping the table to avoid use-after-free
            var cloned_columns = try self.connection.allocator.alloc(storage.Column, table.schema.columns.len);
            var columns_cloned: usize = 0;
            errdefer {
                // Clean up cloned columns on error
                for (cloned_columns[0..columns_cloned]) |column| {
                    self.connection.allocator.free(column.name);
                    if (column.default_value) |default_value| {
                        default_value.deinit(self.connection.allocator);
                    }
                }
                self.connection.allocator.free(cloned_columns);
            }

            for (table.schema.columns, 0..) |column, i| {
                cloned_columns[i] = storage.Column{
                    .name = try self.connection.allocator.dupe(u8, column.name),
                    .data_type = column.data_type,
                    .is_primary_key = column.is_primary_key,
                    .is_nullable = column.is_nullable,
                    .default_value = if (column.default_value) |default_value|
                        try self.cloneStorageDefaultValue(default_value)
                    else
                        null,
                };
                columns_cloned = i + 1;
            }

            var cloned_schema = storage.TableSchema{
                .columns = cloned_columns,
            };

            // Drop and recreate table with remaining data
            try self.connection.storage_engine.dropTable(delete.table_name);
            try self.connection.storage_engine.createTable(table_name, cloned_schema);

            // Clean up temporary schema (storage engine has its own clone now)
            cloned_schema.deinit(self.connection.allocator);

            // Reinsert surviving rows
            const new_table = self.connection.storage_engine.getTable(delete.table_name).?;
            for (surviving_rows.items) |row| {
                // Clone the row for insertion
                var insert_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
                var values_cloned: usize = 0;
                errdefer {
                    for (insert_values[0..values_cloned]) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(insert_values);
                }

                for (row.values, 0..) |value, i| {
                    insert_values[i] = try self.cloneValue(value);
                    values_cloned = i + 1;
                }
                try new_table.insert(storage.Row{ .values = insert_values });
            }
        }

        result.affected_rows = deleted_count;
    }

    /// Evaluate a condition against a row
    fn evaluateCondition(self: *Self, condition: *const ast.Condition, row: *const storage.Row) anyerror!bool {
        return switch (condition.*) {
            .Comparison => |*comp| try self.evaluateComparison(comp, row),
            .Logical => |*logical| {
                const left_result = try self.evaluateCondition(logical.left, row);
                const right_result = try self.evaluateCondition(logical.right, row);

                return switch (logical.operator) {
                    .And => left_result and right_result,
                    .Or => left_result or right_result,
                };
            },
        };
    }

    /// Evaluate a comparison condition
    fn evaluateComparison(self: *Self, comp: *const ast.ComparisonCondition, row: *const storage.Row) !bool {
        const left_value = try self.evaluateExpression(&comp.left, row);
        defer left_value.deinit(self.connection.allocator);

        // Handle IS NULL / IS NOT NULL (don't need to evaluate right side)
        if (comp.operator == .IsNull) {
            return left_value == .Null;
        }
        if (comp.operator == .IsNotNull) {
            return left_value != .Null;
        }

        const right_value = try self.evaluateExpression(&comp.right, row);
        defer right_value.deinit(self.connection.allocator);

        // Handle BETWEEN / NOT BETWEEN
        if (comp.operator == .Between or comp.operator == .NotBetween) {
            if (comp.extra) |extra_expr| {
                const high_value = try self.evaluateExpression(&extra_expr, row);
                defer high_value.deinit(self.connection.allocator);

                const cmp_low = self.compareValues(left_value, right_value);
                const cmp_high = self.compareValues(left_value, high_value);
                const in_range = (cmp_low == .gt or cmp_low == .eq) and (cmp_high == .lt or cmp_high == .eq);

                return if (comp.operator == .Between) in_range else !in_range;
            }
            return false;
        }

        return switch (comp.operator) {
            .Equal => self.compareValues(left_value, right_value) == .eq,
            .NotEqual => self.compareValues(left_value, right_value) != .eq,
            .LessThan => self.compareValues(left_value, right_value) == .lt,
            .LessThanOrEqual => {
                const cmp = self.compareValues(left_value, right_value);
                return cmp == .lt or cmp == .eq;
            },
            .GreaterThan => self.compareValues(left_value, right_value) == .gt,
            .GreaterThanOrEqual => {
                const cmp = self.compareValues(left_value, right_value);
                return cmp == .gt or cmp == .eq;
            },
            .Like => self.evaluateLike(left_value, right_value, false),
            .NotLike => self.evaluateLike(left_value, right_value, true),
            .In => {
                // Simple IN implementation - check equality
                return self.compareValues(left_value, right_value) == .eq;
            },
            .NotIn => {
                // Simple NOT IN implementation
                return self.compareValues(left_value, right_value) != .eq;
            },
            .IsNull, .IsNotNull, .Between, .NotBetween => unreachable, // Already handled above
        };
    }

    /// Evaluate LIKE pattern matching with % and _ wildcards
    fn evaluateLike(self: *Self, value: storage.Value, pattern: storage.Value, negate: bool) bool {
        _ = self;
        const text = switch (value) {
            .Text => |t| t,
            else => return if (negate) true else false,
        };
        const pat = switch (pattern) {
            .Text => |p| p,
            else => return if (negate) true else false,
        };

        const matches = likeMatch(text, pat);
        return if (negate) !matches else matches;
    }

    /// Evaluate CASE WHEN ... THEN ... ELSE ... END expression
    fn evaluateCaseExpression(self: *Self, case_expr: *const ast.CaseExpression, row: *const storage.Row) anyerror!storage.Value {
        // Evaluate each WHEN branch
        for (case_expr.branches) |branch| {
            const condition_result = try self.evaluateCondition(branch.condition, row);
            if (condition_result) {
                // Return the result value for this branch
                return try self.evaluateAstValue(&branch.result, row);
            }
        }

        // No branch matched - return ELSE value or NULL
        if (case_expr.else_result) |else_val| {
            return try self.evaluateAstValue(else_val, row);
        }
        return storage.Value.Null;
    }

    /// Evaluate an AST Value to a storage Value
    fn evaluateAstValue(self: *Self, value: *const ast.Value, row: *const storage.Row) anyerror!storage.Value {
        return switch (value.*) {
            .Integer => |i| storage.Value{ .Integer = i },
            .Text => |t| storage.Value{ .Text = try self.connection.allocator.dupe(u8, t) },
            .Real => |r| storage.Value{ .Real = r },
            .Blob => |b| storage.Value{ .Blob = try self.connection.allocator.dupe(u8, b) },
            .Null => storage.Value.Null,
            .Parameter => |param_index| {
                if (self.parameters) |params| {
                    if (param_index < params.len) {
                        return try self.resolveValue(params[param_index]);
                    }
                }
                return storage.Value.Null;
            },
            .FunctionCall => |function_call| {
                return try self.function_evaluator.evaluateFunction(function_call);
            },
            .Case => |case_expr| {
                return try self.evaluateCaseExpression(&case_expr, row);
            },
        };
    }

    /// Evaluate a function call with row context (resolves column references)
    fn evaluateFunctionWithRow(self: *Self, func_call: ast.FunctionCall, row: *const storage.Row) anyerror!storage.Value {
        const func_name = func_call.name;

        // Convert function name to lowercase for case-insensitive comparison
        const lower_name = try std.ascii.allocLowerString(self.connection.allocator, func_name);
        defer self.connection.allocator.free(lower_name);

        if (std.mem.eql(u8, lower_name, "coalesce")) {
            return self.evalCoalesceWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "nullif")) {
            return self.evalNullifWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "ifnull")) {
            return self.evalIfnullWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "upper")) {
            return self.evalUpperWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "lower")) {
            return self.evalLowerWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "substr") or std.mem.eql(u8, lower_name, "substring")) {
            return self.evalSubstrWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "length")) {
            return self.evalLengthWithRow(func_call.arguments, row);
        } else if (std.mem.eql(u8, lower_name, "trim")) {
            return self.evalTrimWithRow(func_call.arguments, row);
        } else {
            // For other functions, use the regular function evaluator
            return try self.function_evaluator.evaluateFunction(func_call);
        }
    }

    /// COALESCE with row context
    fn evalCoalesceWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len == 0) {
            return error.InvalidArgumentCount;
        }

        for (arguments) |arg| {
            const value = try self.resolveArgumentWithRow(arg, row);
            switch (value) {
                .Null => {
                    value.deinit(self.connection.allocator);
                    continue;
                },
                else => return value,
            }
        }

        return storage.Value.Null;
    }

    /// NULLIF with row context
    fn evalNullifWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len != 2) {
            return error.InvalidArgumentCount;
        }

        const a = try self.resolveArgumentWithRow(arguments[0], row);
        const b = try self.resolveArgumentWithRow(arguments[1], row);
        defer b.deinit(self.connection.allocator);

        // Compare values
        const are_equal = self.valuesEqual(a, b);

        if (are_equal) {
            a.deinit(self.connection.allocator);
            return storage.Value.Null;
        } else {
            return a;
        }
    }

    /// IFNULL with row context
    fn evalIfnullWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len != 2) {
            return error.InvalidArgumentCount;
        }

        const a = try self.resolveArgumentWithRow(arguments[0], row);

        switch (a) {
            .Null => {
                return try self.resolveArgumentWithRow(arguments[1], row);
            },
            else => return a,
        }
    }

    /// Resolve a function argument to a storage value with row context
    fn resolveArgumentWithRow(self: *Self, arg: ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        return switch (arg) {
            .Literal => |value| {
                return switch (value) {
                    .Integer => |i| storage.Value{ .Integer = i },
                    .Real => |r| storage.Value{ .Real = r },
                    .Text => |t| storage.Value{ .Text = try self.connection.allocator.dupe(u8, t) },
                    .Blob => |b| storage.Value{ .Blob = try self.connection.allocator.dupe(u8, b) },
                    .Null => storage.Value.Null,
                    .Parameter => |param_index| {
                        if (self.parameters) |params| {
                            if (param_index < params.len) {
                                return try self.resolveValue(params[param_index]);
                            }
                        }
                        return storage.Value.Null;
                    },
                    .FunctionCall => |func| try self.evaluateFunctionWithRow(func, row),
                    .Case => |case_expr| try self.evaluateCaseExpression(&case_expr, row),
                };
            },
            .String => |s| storage.Value{ .Text = try self.connection.allocator.dupe(u8, s) },
            .Column => |col_name| {
                // Look up column by name in current row using table schema
                if (self.current_table) |table| {
                    for (table.schema.columns, 0..) |col, idx| {
                        if (std.mem.eql(u8, col.name, col_name)) {
                            if (idx < row.values.len) {
                                return try self.cloneValue(row.values[idx]);
                            } else {
                                return storage.Value.Null;
                            }
                        }
                    }
                }
                return storage.Value.Null;
            },
            else => storage.Value.Null,
        };
    }

    /// Compare two storage values for equality
    fn valuesEqual(self: *Self, a: storage.Value, b: storage.Value) bool {
        _ = self;
        // If types don't match, they're not equal
        if (@as(std.meta.Tag(storage.Value), a) != @as(std.meta.Tag(storage.Value), b)) {
            return false;
        }

        return switch (a) {
            .Integer => |i| i == b.Integer,
            .Real => |r| r == b.Real,
            .Text => |t| std.mem.eql(u8, t, b.Text),
            .Blob => |bl| std.mem.eql(u8, bl, b.Blob),
            .Null => true, // NULL = NULL is true for NULLIF
            else => false,
        };
    }

    /// UPPER with row context
    fn evalUpperWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const value = try self.resolveArgumentWithRow(arguments[0], row);
        defer value.deinit(self.connection.allocator);

        switch (value) {
            .Text => |text| {
                const upper = try std.ascii.allocUpperString(self.connection.allocator, text);
                return storage.Value{ .Text = upper };
            },
            .Null => return storage.Value.Null,
            else => return error.InvalidArgumentType,
        }
    }

    /// LOWER with row context
    fn evalLowerWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const value = try self.resolveArgumentWithRow(arguments[0], row);
        defer value.deinit(self.connection.allocator);

        switch (value) {
            .Text => |text| {
                const lower = try std.ascii.allocLowerString(self.connection.allocator, text);
                return storage.Value{ .Text = lower };
            },
            .Null => return storage.Value.Null,
            else => return error.InvalidArgumentType,
        }
    }

    /// SUBSTR(str, start, length) or SUBSTR(str, start) with row context
    fn evalSubstrWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len < 2 or arguments.len > 3) {
            return error.InvalidArgumentCount;
        }

        const str_value = try self.resolveArgumentWithRow(arguments[0], row);
        defer str_value.deinit(self.connection.allocator);

        switch (str_value) {
            .Text => |text| {
                const start_value = try self.resolveArgumentWithRow(arguments[1], row);
                defer start_value.deinit(self.connection.allocator);

                const start: usize = switch (start_value) {
                    .Integer => |i| if (i < 1) 0 else @as(usize, @intCast(i - 1)), // SQL is 1-indexed
                    else => return error.InvalidArgumentType,
                };

                var length: usize = text.len;
                if (arguments.len == 3) {
                    const len_value = try self.resolveArgumentWithRow(arguments[2], row);
                    defer len_value.deinit(self.connection.allocator);
                    length = switch (len_value) {
                        .Integer => |i| if (i < 0) 0 else @as(usize, @intCast(i)),
                        else => return error.InvalidArgumentType,
                    };
                }

                if (start >= text.len) {
                    return storage.Value{ .Text = try self.connection.allocator.dupe(u8, "") };
                }

                const end = @min(start + length, text.len);
                return storage.Value{ .Text = try self.connection.allocator.dupe(u8, text[start..end]) };
            },
            .Null => return storage.Value.Null,
            else => return error.InvalidArgumentType,
        }
    }

    /// LENGTH with row context
    fn evalLengthWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const value = try self.resolveArgumentWithRow(arguments[0], row);
        defer value.deinit(self.connection.allocator);

        switch (value) {
            .Text => |text| {
                return storage.Value{ .Integer = @as(i64, @intCast(text.len)) };
            },
            .Blob => |blob| {
                return storage.Value{ .Integer = @as(i64, @intCast(blob.len)) };
            },
            .Null => return storage.Value.Null,
            else => return error.InvalidArgumentType,
        }
    }

    /// TRIM with row context (removes leading and trailing spaces)
    fn evalTrimWithRow(self: *Self, arguments: []ast.FunctionArgument, row: *const storage.Row) anyerror!storage.Value {
        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const value = try self.resolveArgumentWithRow(arguments[0], row);
        defer value.deinit(self.connection.allocator);

        switch (value) {
            .Text => |text| {
                const trimmed = std.mem.trim(u8, text, " \t\n\r");
                return storage.Value{ .Text = try self.connection.allocator.dupe(u8, trimmed) };
            },
            .Null => return storage.Value.Null,
            else => return error.InvalidArgumentType,
        }
    }

    /// Evaluate an expression against a row
    fn evaluateExpression(self: *Self, expression: *const ast.Expression, row: *const storage.Row) !storage.Value {
        return switch (expression.*) {
            .Column => |col_name| {
                // Look up column by name using current table schema
                if (self.current_table) |table| {
                    for (table.schema.columns, 0..) |col, idx| {
                        if (std.mem.eql(u8, col.name, col_name)) {
                            if (idx < row.values.len) {
                                return try self.cloneValue(row.values[idx]);
                            } else {
                                return storage.Value.Null;
                            }
                        }
                    }
                }
                // Fallback: if no table or column not found, return Null
                return storage.Value.Null;
            },
            .Literal => |value| {
                return switch (value) {
                    .Integer => |i| storage.Value{ .Integer = i },
                    .Text => |t| storage.Value{ .Text = try self.connection.allocator.dupe(u8, t) },
                    .Real => |r| storage.Value{ .Real = r },
                    .Blob => |b| storage.Value{ .Blob = try self.connection.allocator.dupe(u8, b) },
                    .Null => storage.Value.Null,
                    .Parameter => |param_index| {
                        if (self.parameters) |params| {
                            if (param_index < params.len) {
                                return try self.resolveValue(params[param_index]);
                            } else {
                                return error.ParameterIndexOutOfBounds;
                            }
                        } else {
                            return error.NoParametersProvided;
                        }
                    },
                    .FunctionCall => |function_call| {
                        // Evaluate function call immediately
                        return try self.function_evaluator.evaluateFunction(function_call);
                    },
                    .Case => |case_expr| {
                        // Evaluate CASE expression
                        return try self.evaluateCaseExpression(&case_expr, row);
                    },
                };
            },
            .Parameter => |param_index| {
                if (self.parameters) |params| {
                    if (param_index < params.len) {
                        return try self.resolveValue(params[param_index]);
                    } else {
                        return error.ParameterIndexOutOfBounds;
                    }
                } else {
                    return error.NoParametersProvided;
                }
            },
        };
    }

    /// Compare two values
    fn compareValues(self: *Self, left: storage.Value, right: storage.Value) std.math.Order {
        _ = self; // Not needed for value comparison

        return switch (left) {
            .Integer => |l| switch (right) {
                .Integer => |r| std.math.order(l, r),
                .Real => |r| std.math.order(@as(f64, @floatFromInt(l)), r),
                else => .gt, // Non-null values are greater than null
            },
            .Real => |l| switch (right) {
                .Integer => |r| std.math.order(l, @as(f64, @floatFromInt(r))),
                .Real => |r| std.math.order(l, r),
                else => .gt,
            },
            .Text => |l| switch (right) {
                .Text => |r| std.mem.order(u8, l, r),
                else => .gt,
            },
            .Blob => |l| switch (right) {
                .Blob => |r| std.mem.order(u8, l, r),
                else => .gt,
            },
            .Null => switch (right) {
                .Null => .eq,
                else => .lt,
            },
            .Parameter => .gt, // Parameters should have been resolved before comparison
            .FunctionCall => .gt, // Function calls should have been resolved before comparison
            // PostgreSQL compatibility values
            .JSON => |l| switch (right) {
                .JSON => |r| std.mem.order(u8, l, r),
                else => .gt,
            },
            .JSONB => .gt, // Complex comparison - simplified
            .UUID => |l| switch (right) {
                .UUID => |r| std.mem.order(u8, &l, &r),
                else => .gt,
            },
            .Array => .gt, // Complex comparison - simplified
            .Boolean => |l| switch (right) {
                .Boolean => |r| if (l == r) .eq else if (l) .gt else .lt,
                else => .gt,
            },
            .Timestamp => |l| switch (right) {
                .Timestamp => |r| std.math.order(l, r),
                else => .gt,
            },
            .TimestampTZ => |l| switch (right) {
                .TimestampTZ => |r| std.math.order(l.timestamp, r.timestamp),
                else => .gt,
            },
            .Date => |l| switch (right) {
                .Date => |r| std.math.order(l, r),
                else => .gt,
            },
            .Time => |l| switch (right) {
                .Time => |r| std.math.order(l, r),
                else => .gt,
            },
            .Interval => |l| switch (right) {
                .Interval => |r| std.math.order(l, r),
                else => .gt,
            },
            .Numeric => .gt, // Complex comparison - simplified
            .SmallInt => |l| switch (right) {
                .SmallInt => |r| std.math.order(l, r),
                .Integer => |r| std.math.order(@as(i32, l), r),
                else => .gt,
            },
            .BigInt => |l| switch (right) {
                .BigInt => |r| std.math.order(l, r),
                .Integer => |r| std.math.order(@as(i64, r), l),
                else => .gt,
            },
        };
    }

    /// Execute nested loop join (simple but works for all join types)
    fn executeNestedLoopJoin(self: *Self, join: *planner.NestedLoopJoinStep, result: *ExecutionResult) !void {
        // Get tables
        const left_table = self.connection.storage_engine.getTable(join.left_table) orelse {
            return error.TableNotFound;
        };
        const right_table = self.connection.storage_engine.getTable(join.right_table) orelse {
            return error.TableNotFound;
        };

        // Get all rows from both tables
        const left_rows = try left_table.select(self.connection.allocator);
        defer {
            for (left_rows) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            self.connection.allocator.free(left_rows);
        }

        const right_rows = try right_table.select(self.connection.allocator);
        defer {
            for (right_rows) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            self.connection.allocator.free(right_rows);
        }

        // Perform join logic based on join type
        for (left_rows) |left_row| {
            var matched = false;

            for (right_rows) |right_row| {
                // Create combined row for condition evaluation
                const combined_row = try self.combineRows(&left_row, &right_row);
                defer {
                    for (combined_row.values) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(combined_row.values);
                }

                // Check join condition
                if (try self.evaluateCondition(&join.condition, &combined_row)) {
                    matched = true;
                    // Add the combined row to results
                    const final_row = try self.combineRows(&left_row, &right_row);
                    try result.rows.append(self.connection.allocator, final_row);
                }
            }

            // Handle LEFT JOIN case where no match found
            if (!matched and join.join_type == .Left) {
                const null_right_row = try self.createNullRow(right_rows[0].values.len);
                defer {
                    for (null_right_row.values) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(null_right_row.values);
                }

                const final_row = try self.combineRows(&left_row, &null_right_row);
                try result.rows.append(self.connection.allocator, final_row);
            }
        }

        // Handle RIGHT JOIN - iterate from right side
        if (join.join_type == .Right or join.join_type == .Full) {
            for (right_rows) |right_row| {
                var matched = false;

                for (left_rows) |left_row| {
                    const combined_row = try self.combineRows(&left_row, &right_row);
                    defer {
                        for (combined_row.values) |value| {
                            value.deinit(self.connection.allocator);
                        }
                        self.connection.allocator.free(combined_row.values);
                    }

                    if (try self.evaluateCondition(&join.condition, &combined_row)) {
                        matched = true;
                        break; // We already added this in the LEFT side iteration for FULL
                    }
                }

                // Add unmatched RIGHT rows for RIGHT and FULL joins
                if (!matched and (join.join_type == .Right or join.join_type == .Full)) {
                    const null_left_row = try self.createNullRow(left_rows[0].values.len);
                    defer {
                        for (null_left_row.values) |value| {
                            value.deinit(self.connection.allocator);
                        }
                        self.connection.allocator.free(null_left_row.values);
                    }

                    const final_row = try self.combineRows(&null_left_row, &right_row);
                    try result.rows.append(self.connection.allocator, final_row);
                }
            }
        }
    }

    /// Execute hash join (optimized for equi-joins)
    fn executeHashJoin(self: *Self, join: *planner.HashJoinStep, result: *ExecutionResult) !void {
        // Get tables
        const left_table = self.connection.storage_engine.getTable(join.left_table) orelse {
            return error.TableNotFound;
        };
        const right_table = self.connection.storage_engine.getTable(join.right_table) orelse {
            return error.TableNotFound;
        };

        // Get all rows from both tables
        const left_rows = try left_table.select(self.connection.allocator);
        defer {
            for (left_rows) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            self.connection.allocator.free(left_rows);
        }

        const right_rows = try right_table.select(self.connection.allocator);
        defer {
            for (right_rows) |row| {
                for (row.values) |value| {
                    value.deinit(self.connection.allocator);
                }
                self.connection.allocator.free(row.values);
            }
            self.connection.allocator.free(right_rows);
        }

        // Get column indices for join keys
        const left_key_idx = left_table.getColumnIndex(join.left_key_column) orelse {
            // Fall back to nested loop if column not found
            var nested_join = planner.NestedLoopJoinStep{
                .join_type = join.join_type,
                .left_table = join.left_table,
                .right_table = join.right_table,
                .condition = join.condition,
            };
            return self.executeNestedLoopJoin(&nested_join, result);
        };

        const right_key_idx = right_table.getColumnIndex(join.right_key_column) orelse {
            // Fall back to nested loop if column not found
            var nested_join = planner.NestedLoopJoinStep{
                .join_type = join.join_type,
                .left_table = join.left_table,
                .right_table = join.right_table,
                .condition = join.condition,
            };
            return self.executeNestedLoopJoin(&nested_join, result);
        };

        // Build hash table from right table (the "build" side)
        // Key: hash of join key value, Value: list of row indices with that key
        var hash_map = std.AutoHashMap(u64, std.ArrayList(usize)).init(self.connection.allocator);
        defer {
            var iterator = hash_map.valueIterator();
            while (iterator.next()) |list| {
                list.deinit(self.connection.allocator);
            }
            hash_map.deinit();
        }

        // Build phase: hash the right table's join key values
        for (right_rows, 0..) |row, row_idx| {
            if (right_key_idx < row.values.len) {
                const key_value = row.values[right_key_idx];
                const hash = hashValue(key_value);

                const entry = try hash_map.getOrPut(hash);
                if (!entry.found_existing) {
                    entry.value_ptr.* = .{};
                }
                try entry.value_ptr.append(self.connection.allocator, row_idx);
            }
        }

        // Probe phase: for each left row, look up matching right rows
        for (left_rows) |left_row| {
            if (left_key_idx >= left_row.values.len) continue;

            const left_key_value = left_row.values[left_key_idx];
            const hash = hashValue(left_key_value);

            var matched = false;

            if (hash_map.get(hash)) |matching_indices| {
                for (matching_indices.items) |right_idx| {
                    const right_row = right_rows[right_idx];

                    // Verify the actual values match (not just hash)
                    if (right_key_idx < right_row.values.len) {
                        const right_key_value = right_row.values[right_key_idx];
                        if (hashValuesEqual(left_key_value, right_key_value)) {
                            // Found a match - combine rows
                            const combined = try self.combineRows(&left_row, &right_row);
                            try result.rows.append(self.connection.allocator, combined);
                            matched = true;
                        }
                    }
                }
            }

            // Handle LEFT JOIN - include left row even if no match
            if (!matched and join.join_type == .Left) {
                const right_null_count = if (right_rows.len > 0) right_rows[0].values.len else 0;
                const combined = try self.combineRowWithNulls(&left_row, right_null_count);
                try result.rows.append(self.connection.allocator, combined);
            }
        }

        // Handle RIGHT JOIN - include unmatched right rows
        if (join.join_type == .Right or join.join_type == .Full) {
            var matched_right = try self.connection.allocator.alloc(bool, right_rows.len);
            defer self.connection.allocator.free(matched_right);
            @memset(matched_right, false);

            // Re-probe to find matched right rows
            for (left_rows) |left_row| {
                if (left_key_idx >= left_row.values.len) continue;
                const left_key_value = left_row.values[left_key_idx];
                const hash = hashValue(left_key_value);

                if (hash_map.get(hash)) |matching_indices| {
                    for (matching_indices.items) |right_idx| {
                        const right_row = right_rows[right_idx];
                        if (right_key_idx < right_row.values.len) {
                            const right_key_value = right_row.values[right_key_idx];
                            if (hashValuesEqual(left_key_value, right_key_value)) {
                                matched_right[right_idx] = true;
                            }
                        }
                    }
                }
            }

            // Add unmatched right rows with NULL left columns
            const left_null_count = if (left_rows.len > 0) left_rows[0].values.len else 0;
            for (right_rows, 0..) |right_row, idx| {
                if (!matched_right[idx]) {
                    const combined = try self.combineNullsWithRow(left_null_count, &right_row);
                    try result.rows.append(self.connection.allocator, combined);
                }
            }
        }
    }

    /// Hash a storage value for hash join
    fn hashValue(value: storage.Value) u64 {
        var hasher = std.hash.Wyhash.init(0);
        switch (value) {
            .Integer => |i| hasher.update(std.mem.asBytes(&i)),
            .Real => |r| hasher.update(std.mem.asBytes(&r)),
            .Text => |t| hasher.update(t),
            .Blob => |b| hasher.update(b),
            .Null => hasher.update(&[_]u8{0}),
            .Boolean => |b| hasher.update(&[_]u8{if (b) 1 else 0}),
            else => hasher.update(&[_]u8{0}),
        }
        return hasher.final();
    }

    /// Check if two values are equal (for hash join verification)
    fn hashValuesEqual(a: storage.Value, b: storage.Value) bool {
        return switch (a) {
            .Integer => |ia| switch (b) {
                .Integer => |ib| ia == ib,
                else => false,
            },
            .Real => |ra| switch (b) {
                .Real => |rb| ra == rb,
                else => false,
            },
            .Text => |ta| switch (b) {
                .Text => |tb| std.mem.eql(u8, ta, tb),
                else => false,
            },
            .Blob => |ba| switch (b) {
                .Blob => |bb| std.mem.eql(u8, ba, bb),
                else => false,
            },
            .Null => switch (b) {
                .Null => true,
                else => false,
            },
            .Boolean => |ba| switch (b) {
                .Boolean => |bb| ba == bb,
                else => false,
            },
            else => false,
        };
    }

    /// Combine a row with NULL values (for LEFT JOIN)
    fn combineRowWithNulls(self: *Self, left: *const storage.Row, right_null_count: usize) !storage.Row {
        const total_cols = left.values.len + right_null_count;
        var combined_values = try self.connection.allocator.alloc(storage.Value, total_cols);

        for (left.values, 0..) |value, i| {
            combined_values[i] = try value.clone(self.connection.allocator);
        }

        for (left.values.len..total_cols) |i| {
            combined_values[i] = storage.Value.Null;
        }

        return storage.Row{ .values = combined_values };
    }

    /// Combine NULL values with a row (for RIGHT JOIN)
    fn combineNullsWithRow(self: *Self, left_null_count: usize, right: *const storage.Row) !storage.Row {
        const total_cols = left_null_count + right.values.len;
        var combined_values = try self.connection.allocator.alloc(storage.Value, total_cols);

        for (0..left_null_count) |i| {
            combined_values[i] = storage.Value.Null;
        }

        for (right.values, 0..) |value, i| {
            combined_values[left_null_count + i] = try value.clone(self.connection.allocator);
        }

        return storage.Row{ .values = combined_values };
    }

    /// Combine two rows into a single row
    fn combineRows(self: *Self, left_row: *const storage.Row, right_row: *const storage.Row) !storage.Row {
        const total_columns = left_row.values.len + right_row.values.len;
        var combined_values = try self.connection.allocator.alloc(storage.Value, total_columns);
        var values_cloned: usize = 0;
        errdefer {
            for (combined_values[0..values_cloned]) |value| {
                value.deinit(self.connection.allocator);
            }
            self.connection.allocator.free(combined_values);
        }

        // Copy left row values
        for (left_row.values, 0..) |value, i| {
            combined_values[i] = try self.cloneValue(value);
            values_cloned = i + 1;
        }

        // Copy right row values
        for (right_row.values, 0..) |value, i| {
            combined_values[left_row.values.len + i] = try self.cloneValue(value);
            values_cloned = left_row.values.len + i + 1;
        }

        return storage.Row{ .values = combined_values };
    }

    /// Create a row with all NULL values
    fn createNullRow(self: *Self, column_count: usize) !storage.Row {
        const null_values = try self.connection.allocator.alloc(storage.Value, column_count);
        for (null_values) |*value| {
            value.* = storage.Value.Null;
        }
        return storage.Row{ .values = null_values };
    }

    /// Execute aggregate operation
    fn executeAggregate(self: *Self, agg: *planner.AggregateStep, result: *ExecutionResult) !void {
        // Get table schema for column lookup
        const table = blk: {
            var table_iter = self.connection.storage_engine.tables.iterator();
            if (table_iter.next()) |entry| {
                break :blk entry.value_ptr.*;
            }
            break :blk null;
        };

        for (agg.aggregates) |aggregate_op| {
            // Find the column index for the aggregate
            const col_idx: usize = if (aggregate_op.column) |col_name| idx: {
                if (table) |t| {
                    for (t.schema.columns, 0..) |col, i| {
                        if (std.mem.eql(u8, col.name, col_name)) {
                            break :idx i;
                        }
                    }
                }
                break :idx 0;
            } else 0;

            switch (aggregate_op.function_type) {
                .Count => {
                    // COUNT(*) - count all rows in the result
                    const count = result.rows.items.len;
                    const result_value = storage.Value{ .Integer = @intCast(count) };
                    try self.finishAggregateResult(result, result_value);
                },
                .Sum => {
                    // SUM(column) - sum all numeric values in the specified column
                    var sum: f64 = 0.0;
                    for (result.rows.items) |row| {
                        if (col_idx < row.values.len) {
                            switch (row.values[col_idx]) {
                                .Integer => |i| sum += @floatFromInt(i),
                                .Real => |r| sum += r,
                                else => {}, // Skip non-numeric values
                            }
                        }
                    }
                    const result_value = storage.Value{ .Real = sum };
                    try self.finishAggregateResult(result, result_value);
                },
                .Avg => {
                    // AVG(column) - average of all numeric values
                    var sum: f64 = 0.0;
                    var count: u32 = 0;
                    for (result.rows.items) |row| {
                        if (col_idx < row.values.len) {
                            switch (row.values[col_idx]) {
                                .Integer => |i| {
                                    sum += @floatFromInt(i);
                                    count += 1;
                                },
                                .Real => |r| {
                                    sum += r;
                                    count += 1;
                                },
                                else => {}, // Skip non-numeric values
                            }
                        }
                    }
                    const avg = if (count > 0) sum / @as(f64, @floatFromInt(count)) else 0.0;
                    const result_value = storage.Value{ .Real = avg };
                    try self.finishAggregateResult(result, result_value);
                },
                .Min => {
                    // MIN(column) - minimum value
                    var min_value: ?storage.Value = null;
                    for (result.rows.items) |row| {
                        if (col_idx < row.values.len) {
                            const current_value = row.values[col_idx];
                            if (min_value == null) {
                                min_value = try self.cloneValue(current_value);
                            } else {
                                if (self.compareValues(current_value, min_value.?) == .lt) {
                                    min_value.?.deinit(self.connection.allocator);
                                    min_value = try self.cloneValue(current_value);
                                }
                            }
                        }
                    }
                    const result_value = min_value orelse storage.Value.Null;
                    try self.finishAggregateResult(result, result_value);
                },
                .Max => {
                    // MAX(column) - maximum value
                    var max_value: ?storage.Value = null;
                    for (result.rows.items) |row| {
                        if (col_idx < row.values.len) {
                            const current_value = row.values[col_idx];
                            if (max_value == null) {
                                max_value = try self.cloneValue(current_value);
                            } else {
                                if (self.compareValues(current_value, max_value.?) == .gt) {
                                    max_value.?.deinit(self.connection.allocator);
                                    max_value = try self.cloneValue(current_value);
                                }
                            }
                        }
                    }
                    const result_value = max_value orelse storage.Value.Null;
                    try self.finishAggregateResult(result, result_value);
                },
                .GroupConcat => {
                    // GROUP_CONCAT - concatenate all values with comma separator
                    var concat: std.ArrayList(u8) = .{};
                    defer concat.deinit(self.connection.allocator);
                    var first = true;
                    for (result.rows.items) |row| {
                        if (row.values.len > 0) {
                            if (!first) try concat.appendSlice(self.connection.allocator, ",");
                            first = false;
                            switch (row.values[0]) {
                                .Text => |t| try concat.appendSlice(self.connection.allocator, t),
                                .Integer => |i| {
                                    var buf: [32]u8 = undefined;
                                    const slice = std.fmt.bufPrint(&buf, "{d}", .{i}) catch "0";
                                    try concat.appendSlice(self.connection.allocator, slice);
                                },
                                .Real => |r| {
                                    var buf: [64]u8 = undefined;
                                    const slice = std.fmt.bufPrint(&buf, "{d}", .{r}) catch "0";
                                    try concat.appendSlice(self.connection.allocator, slice);
                                },
                                else => {},
                            }
                        }
                    }
                    const result_value = storage.Value{ .Text = try self.connection.allocator.dupe(u8, concat.items) };
                    try self.finishAggregateResult(result, result_value);
                },
                .CountDistinct => {
                    // COUNT(DISTINCT column) - count unique values
                    var seen = std.StringHashMap(void).init(self.connection.allocator);
                    defer {
                        var iter = seen.iterator();
                        while (iter.next()) |entry| {
                            self.connection.allocator.free(entry.key_ptr.*);
                        }
                        seen.deinit();
                    }
                    for (result.rows.items) |row| {
                        if (row.values.len > 0) {
                            var key_buf: std.ArrayList(u8) = .{};
                            defer key_buf.deinit(self.connection.allocator);
                            try self.appendValueToKey(&key_buf, row.values[0]);
                            const key = try self.connection.allocator.dupe(u8, key_buf.items);
                            const gop = try seen.getOrPut(key);
                            if (gop.found_existing) {
                                self.connection.allocator.free(key);
                            }
                        }
                    }
                    const result_value = storage.Value{ .Integer = @intCast(seen.count()) };
                    try self.finishAggregateResult(result, result_value);
                },
            }
        }
    }

    /// Helper function to finish aggregate result
    fn finishAggregateResult(self: *Self, result: *ExecutionResult, aggregate_value: storage.Value) !void {
        // Clear existing rows and add the aggregate result
        for (result.rows.items) |row| {
            for (row.values) |value| {
                value.deinit(self.connection.allocator);
            }
            self.connection.allocator.free(row.values);
        }
        result.rows.clearAndFree(self.connection.allocator);

        // Create a single row with the aggregate result
        var aggregate_row_values = try self.connection.allocator.alloc(storage.Value, 1);
        aggregate_row_values[0] = aggregate_value;

        try result.rows.append(self.connection.allocator, storage.Row{ .values = aggregate_row_values });
    }

    /// Execute group by operation
    fn executeGroupBy(self: *Self, group: *planner.GroupByStep, result: *ExecutionResult) !void {
        // Group rows by the specified columns
        // Key: hash of group column values -> list of rows in that group
        var groups = std.StringHashMap(std.ArrayList(storage.Row)).init(self.connection.allocator);
        defer {
            var iter = groups.iterator();
            while (iter.next()) |entry| {
                self.connection.allocator.free(entry.key_ptr.*);
                entry.value_ptr.deinit(self.connection.allocator);
            }
            groups.deinit();
        }

        // Get table schema for column index lookup
        const table = if (result.rows.items.len > 0) blk: {
            // Try to find table from context - for now use first table in storage
            var table_iter = self.connection.storage_engine.tables.iterator();
            if (table_iter.next()) |entry| {
                break :blk entry.value_ptr.*;
            }
            break :blk null;
        } else null;

        // Build groups
        for (result.rows.items) |row| {
            // Build group key from specified columns
            var key_parts: std.ArrayList(u8) = .{};
            defer key_parts.deinit(self.connection.allocator);

            for (group.group_columns) |col_name| {
                const col_idx = if (table) |t| self.findColumnIndex(t, col_name) else null;
                const value = if (col_idx) |idx| blk: {
                    if (idx < row.values.len) {
                        break :blk row.values[idx];
                    }
                    break :blk storage.Value.Null;
                } else if (row.values.len > 0) row.values[0] else storage.Value.Null;

                // Append value representation to key
                try self.appendValueToKey(&key_parts, value);
                try key_parts.append(self.connection.allocator, '|');
            }

            const key = try self.connection.allocator.dupe(u8, key_parts.items);
            errdefer self.connection.allocator.free(key);

            // Get or create group
            const gop = try groups.getOrPut(key);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{};
            } else {
                // Key already exists, free the duplicate
                self.connection.allocator.free(key);
            }

            // Clone row and add to group
            var cloned_values = try self.connection.allocator.alloc(storage.Value, row.values.len);
            for (row.values, 0..) |v, i| {
                cloned_values[i] = try self.cloneValue(v);
            }
            try gop.value_ptr.append(self.connection.allocator, storage.Row{ .values = cloned_values });
        }

        // Clear original rows
        for (result.rows.items) |row| {
            for (row.values) |value| {
                value.deinit(self.connection.allocator);
            }
            self.connection.allocator.free(row.values);
        }
        result.rows.clearAndFree(self.connection.allocator);

        // Process each group and compute aggregates
        var group_iter = groups.iterator();
        while (group_iter.next()) |entry| {
            const group_rows = entry.value_ptr.items;
            if (group_rows.len == 0) continue;

            // Build result row: group columns + aggregates
            const result_col_count = group.group_columns.len + group.aggregates.len;
            var result_values = try self.connection.allocator.alloc(storage.Value, result_col_count);
            var values_set: usize = 0;
            errdefer {
                for (result_values[0..values_set]) |v| v.deinit(self.connection.allocator);
                self.connection.allocator.free(result_values);
            }

            // Add group column values from first row
            for (group.group_columns, 0..) |col_name, i| {
                const col_idx = if (table) |t| self.findColumnIndex(t, col_name) else null;
                const value = if (col_idx) |idx| blk: {
                    if (idx < group_rows[0].values.len) {
                        break :blk group_rows[0].values[idx];
                    }
                    break :blk storage.Value.Null;
                } else if (group_rows[0].values.len > 0) group_rows[0].values[0] else storage.Value.Null;

                result_values[i] = try self.cloneValue(value);
                values_set = i + 1;
            }

            // Compute aggregates for this group
            for (group.aggregates, 0..) |agg, i| {
                const agg_value = try self.computeAggregate(agg, group_rows, table);
                result_values[group.group_columns.len + i] = agg_value;
                values_set = group.group_columns.len + i + 1;
            }

            try result.rows.append(self.connection.allocator, storage.Row{ .values = result_values });

            // Clean up group rows
            for (group_rows) |row| {
                for (row.values) |v| v.deinit(self.connection.allocator);
                self.connection.allocator.free(row.values);
            }
        }
    }

    /// Find column index by name in table schema
    fn findColumnIndex(self: *Self, table: *storage.Table, col_name: []const u8) ?usize {
        _ = self;
        for (table.schema.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, col_name)) {
                return i;
            }
        }
        return null;
    }

    /// Append value representation to key buffer for grouping
    fn appendValueToKey(self: *Self, key: *std.ArrayList(u8), value: storage.Value) !void {
        switch (value) {
            .Integer => |i| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{i}) catch "0";
                try key.appendSlice(self.connection.allocator, slice);
            },
            .Text => |t| try key.appendSlice(self.connection.allocator, t),
            .Real => |r| {
                var buf: [64]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{r}) catch "0";
                try key.appendSlice(self.connection.allocator, slice);
            },
            .Null => try key.appendSlice(self.connection.allocator, "NULL"),
            else => try key.appendSlice(self.connection.allocator, "?"),
        }
    }

    /// Compute aggregate value for a group of rows
    fn computeAggregate(self: *Self, agg: planner.AggregateOperation, rows: []storage.Row, table: ?*storage.Table) !storage.Value {
        const col_idx: ?usize = if (agg.column) |col_name| blk: {
            if (table) |t| {
                break :blk self.findColumnIndex(t, col_name);
            }
            break :blk null;
        } else null;

        switch (agg.function_type) {
            .Count => {
                return storage.Value{ .Integer = @intCast(rows.len) };
            },
            .Sum => {
                var sum: f64 = 0.0;
                for (rows) |row| {
                    const val = self.getAggregateColumnValue(row, col_idx);
                    switch (val) {
                        .Integer => |i| sum += @floatFromInt(i),
                        .Real => |r| sum += r,
                        else => {},
                    }
                }
                return storage.Value{ .Real = sum };
            },
            .Avg => {
                var sum: f64 = 0.0;
                var count: u32 = 0;
                for (rows) |row| {
                    const val = self.getAggregateColumnValue(row, col_idx);
                    switch (val) {
                        .Integer => |i| {
                            sum += @floatFromInt(i);
                            count += 1;
                        },
                        .Real => |r| {
                            sum += r;
                            count += 1;
                        },
                        else => {},
                    }
                }
                return storage.Value{ .Real = if (count > 0) sum / @as(f64, @floatFromInt(count)) else 0.0 };
            },
            .Min => {
                var min_val: ?storage.Value = null;
                for (rows) |row| {
                    const val = self.getAggregateColumnValue(row, col_idx);
                    if (min_val == null) {
                        min_val = try self.cloneValue(val);
                    } else if (self.compareValues(val, min_val.?) == .lt) {
                        min_val.?.deinit(self.connection.allocator);
                        min_val = try self.cloneValue(val);
                    }
                }
                return min_val orelse storage.Value.Null;
            },
            .Max => {
                var max_val: ?storage.Value = null;
                for (rows) |row| {
                    const val = self.getAggregateColumnValue(row, col_idx);
                    if (max_val == null) {
                        max_val = try self.cloneValue(val);
                    } else if (self.compareValues(val, max_val.?) == .gt) {
                        max_val.?.deinit(self.connection.allocator);
                        max_val = try self.cloneValue(val);
                    }
                }
                return max_val orelse storage.Value.Null;
            },
            .GroupConcat => {
                var concat: std.ArrayList(u8) = .{};
                defer concat.deinit(self.connection.allocator);
                var first = true;
                for (rows) |row| {
                    const val = self.getAggregateColumnValue(row, col_idx);
                    if (!first) try concat.appendSlice(self.connection.allocator, ",");
                    first = false;
                    switch (val) {
                        .Text => |t| try concat.appendSlice(self.connection.allocator, t),
                        .Integer => |i| {
                            var buf: [32]u8 = undefined;
                            const slice = std.fmt.bufPrint(&buf, "{d}", .{i}) catch "0";
                            try concat.appendSlice(self.connection.allocator, slice);
                        },
                        else => {},
                    }
                }
                return storage.Value{ .Text = try self.connection.allocator.dupe(u8, concat.items) };
            },
            .CountDistinct => {
                var seen = std.StringHashMap(void).init(self.connection.allocator);
                defer seen.deinit();
                for (rows) |row| {
                    const val = self.getAggregateColumnValue(row, col_idx);
                    var key_buf: std.ArrayList(u8) = .{};
                    defer key_buf.deinit(self.connection.allocator);
                    try self.appendValueToKey(&key_buf, val);
                    const key = try self.connection.allocator.dupe(u8, key_buf.items);
                    const gop = try seen.getOrPut(key);
                    if (gop.found_existing) {
                        self.connection.allocator.free(key);
                    }
                }
                // Clean up keys
                var iter = seen.iterator();
                while (iter.next()) |entry| {
                    self.connection.allocator.free(entry.key_ptr.*);
                }
                return storage.Value{ .Integer = @intCast(seen.count()) };
            },
        }
    }

    /// Get column value for aggregate computation
    fn getAggregateColumnValue(self: *Self, row: storage.Row, col_idx: ?usize) storage.Value {
        _ = self;
        if (col_idx) |idx| {
            if (idx < row.values.len) {
                return row.values[idx];
            }
        }
        // Default to first column if no specific column
        if (row.values.len > 0) {
            return row.values[0];
        }
        return storage.Value.Null;
    }

    /// Execute BEGIN TRANSACTION
    fn executeBeginTransaction(self: *Self, result: *ExecutionResult) !void {
        _ = result;
        try self.connection.beginTransaction();
    }

    /// Execute COMMIT
    fn executeCommit(self: *Self, result: *ExecutionResult) !void {
        _ = result;
        try self.connection.commitTransaction();
    }

    /// Execute ROLLBACK
    fn executeRollback(self: *Self, result: *ExecutionResult) !void {
        _ = result;
        try self.connection.rollbackTransaction();
    }

    /// Execute CREATE INDEX
    fn executeCreateIndex(self: *Self, create_idx: *planner.CreateIndexStep, result: *ExecutionResult) !void {
        _ = result;

        // Check if table exists
        const table = self.connection.storage_engine.getTable(create_idx.table_name) orelse {
            return error.TableNotFound;
        };

        // Verify columns exist
        for (create_idx.columns) |col_name| {
            var found = false;
            for (table.schema.columns) |column| {
                if (std.mem.eql(u8, column.name, col_name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return error.ColumnNotFound;
            }
        }

        // Check if index already exists
        if (self.connection.storage_engine.getIndex(create_idx.index_name) != null) {
            if (create_idx.if_not_exists) {
                return; // Silently skip if IF NOT EXISTS is specified
            }
            return error.IndexAlreadyExists;
        }

        // Create the index in storage engine
        try self.connection.storage_engine.createIndex(
            create_idx.index_name,
            create_idx.table_name,
            create_idx.columns,
            create_idx.unique,
        );
    }

    /// Execute DROP INDEX
    fn executeDropIndex(self: *Self, drop_idx: *planner.DropIndexStep, result: *ExecutionResult) !void {
        _ = result;

        // Check if index exists
        if (self.connection.storage_engine.getIndex(drop_idx.index_name) == null) {
            if (drop_idx.if_exists) {
                return; // Silently skip if IF EXISTS is specified
            }
            return error.IndexNotFound;
        }

        // Drop the index
        try self.connection.storage_engine.dropIndex(drop_idx.index_name);
    }

    /// Execute DROP TABLE
    fn executeDropTable(self: *Self, drop_tbl: *planner.DropTableStep, result: *ExecutionResult) !void {
        _ = result;

        // Check if table exists
        if (self.connection.storage_engine.getTable(drop_tbl.table_name) == null) {
            if (drop_tbl.if_exists) {
                return; // Silently skip if IF EXISTS is specified
            }
            return error.TableNotFound;
        }

        // Drop the table
        try self.connection.storage_engine.dropTable(drop_tbl.table_name);
    }

    /// Execute PRAGMA statement
    fn executePragma(self: *Self, pragma: *planner.PragmaStep, result: *ExecutionResult) !void {
        const allocator = self.connection.allocator;

        // Handle different PRAGMA commands
        if (std.ascii.eqlIgnoreCase(pragma.name, "table_info")) {
            // PRAGMA table_info(table_name)
            // Returns: cid, name, type, notnull, dflt_value, pk
            const table_name = pragma.argument orelse return error.PragmaRequiresArgument;

            const table = self.connection.storage_engine.getTable(table_name) orelse {
                return error.TableNotFound;
            };

            // Generate rows for each column in the table
            for (table.schema.columns, 0..) |column, cid| {
                var row_values: std.ArrayList(storage.Value) = .{};

                // cid (column index)
                try row_values.append(allocator, storage.Value{ .Integer = @intCast(cid) });

                // name (column name)
                try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, column.name) });

                // type (data type as string)
                const type_str = switch (column.data_type) {
                    .Integer => "INTEGER",
                    .Text => "TEXT",
                    .Real => "REAL",
                    .Blob => "BLOB",
                    .JSON => "JSON",
                    .JSONB => "JSONB",
                    .UUID => "UUID",
                    .Array => "ARRAY",
                    .TimestampTZ => "TIMESTAMPTZ",
                    .Interval => "INTERVAL",
                    .Numeric => "NUMERIC",
                    .Boolean => "BOOLEAN",
                    .Timestamp => "TIMESTAMP",
                    .Date => "DATE",
                    .Time => "TIME",
                    .SmallInt => "SMALLINT",
                    .BigInt => "BIGINT",
                    .Varchar => "VARCHAR",
                    .Char => "CHAR",
                };
                try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, type_str) });

                // notnull (1 if NOT NULL, 0 otherwise)
                try row_values.append(allocator, storage.Value{ .Integer = if (column.is_nullable) 0 else 1 });

                // dflt_value (default value or NULL)
                if (column.default_value) |default| {
                    switch (default) {
                        .Literal => |lit_value| {
                            try row_values.append(allocator, try lit_value.clone(allocator));
                        },
                        .FunctionCall => |func| {
                            // Represent function call as text
                            try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, func.name) });
                        },
                    }
                } else {
                    try row_values.append(allocator, storage.Value.Null);
                }

                // pk (1 if primary key, 0 otherwise)
                try row_values.append(allocator, storage.Value{ .Integer = if (column.is_primary_key) 1 else 0 });

                try result.rows.append(allocator, storage.Row{
                    .values = try row_values.toOwnedSlice(allocator),
                });
            }
        } else if (std.ascii.eqlIgnoreCase(pragma.name, "database_list")) {
            // PRAGMA database_list
            // Returns: seq, name, file
            var row_values: std.ArrayList(storage.Value) = .{};
            try row_values.append(allocator, storage.Value{ .Integer = 0 }); // seq
            try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, "main") }); // name
            try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, ":memory:") }); // file

            try result.rows.append(allocator, storage.Row{
                .values = try row_values.toOwnedSlice(allocator),
            });
        } else if (std.ascii.eqlIgnoreCase(pragma.name, "table_list")) {
            // PRAGMA table_list
            // Returns: schema, name, type, ncol, wr, strict
            var table_iter = self.connection.storage_engine.tables.iterator();
            while (table_iter.next()) |entry| {
                const table_name = entry.key_ptr.*;
                const table = entry.value_ptr.*;
                var row_values: std.ArrayList(storage.Value) = .{};

                try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, "main") }); // schema
                try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, table_name) }); // name
                try row_values.append(allocator, storage.Value{ .Text = try allocator.dupe(u8, "table") }); // type
                try row_values.append(allocator, storage.Value{ .Integer = @intCast(table.schema.columns.len) }); // ncol
                try row_values.append(allocator, storage.Value{ .Integer = 0 }); // wr (without rowid)
                try row_values.append(allocator, storage.Value{ .Integer = 0 }); // strict

                try result.rows.append(allocator, storage.Row{
                    .values = try row_values.toOwnedSlice(allocator),
                });
            }
        } else {
            return error.UnknownPragma;
        }
    }

    /// Execute EXPLAIN / EXPLAIN QUERY PLAN statement
    fn executeExplain(self: *Self, explain: *planner.ExplainStep, result: *ExecutionResult) !void {
        const allocator = self.connection.allocator;

        // EXPLAIN QUERY PLAN returns: id, parent, notused, detail
        // EXPLAIN returns: addr, opcode, p1, p2, p3, p4, p5, comment
        // We'll use the simpler EXPLAIN QUERY PLAN format for now

        for (explain.inner_steps, 0..) |step, step_idx| {
            var row_values: std.ArrayList(storage.Value) = .{};

            // id (step index)
            try row_values.append(allocator, storage.Value{ .Integer = @intCast(step_idx) });

            // parent (0 for top-level steps)
            try row_values.append(allocator, storage.Value{ .Integer = 0 });

            // notused (always 0 for compatibility)
            try row_values.append(allocator, storage.Value{ .Integer = 0 });

            // detail (description of what this step does)
            const detail = try self.describeStep(&step, allocator);
            try row_values.append(allocator, storage.Value{ .Text = detail });

            try result.rows.append(allocator, storage.Row{
                .values = try row_values.toOwnedSlice(allocator),
            });
        }
    }

    /// Generate a human-readable description of an execution step
    fn describeStep(self: *Self, step: *const planner.ExecutionStep, allocator: std.mem.Allocator) ![]const u8 {
        _ = self;
        return switch (step.*) {
            .TableScan => |scan| try std.fmt.allocPrint(allocator, "SCAN TABLE {s}", .{scan.table_name}),
            .Filter => try allocator.dupe(u8, "FILTER"),
            .Project => try allocator.dupe(u8, "PROJECT"),
            .Limit => |limit| try std.fmt.allocPrint(allocator, "LIMIT {d}", .{limit.count}),
            .Insert => |insert| try std.fmt.allocPrint(allocator, "INSERT INTO {s}", .{insert.table_name}),
            .CreateTable => |create| try std.fmt.allocPrint(allocator, "CREATE TABLE {s}", .{create.table_name}),
            .Update => |update| try std.fmt.allocPrint(allocator, "UPDATE {s}", .{update.table_name}),
            .Delete => |del| try std.fmt.allocPrint(allocator, "DELETE FROM {s}", .{del.table_name}),
            .NestedLoopJoin => |join| try std.fmt.allocPrint(allocator, "NESTED LOOP JOIN {s} WITH {s}", .{ join.left_table, join.right_table }),
            .HashJoin => |join| try std.fmt.allocPrint(allocator, "HASH JOIN {s} WITH {s}", .{ join.left_table, join.right_table }),
            .Aggregate => try allocator.dupe(u8, "AGGREGATE"),
            .GroupBy => try allocator.dupe(u8, "GROUP BY"),
            .BeginTransaction => try allocator.dupe(u8, "BEGIN TRANSACTION"),
            .Commit => try allocator.dupe(u8, "COMMIT"),
            .Rollback => try allocator.dupe(u8, "ROLLBACK"),
            .CreateIndex => |idx| try std.fmt.allocPrint(allocator, "CREATE INDEX {s} ON {s}", .{ idx.index_name, idx.table_name }),
            .DropIndex => |idx| try std.fmt.allocPrint(allocator, "DROP INDEX {s}", .{idx.index_name}),
            .DropTable => |drop| try std.fmt.allocPrint(allocator, "DROP TABLE {s}", .{drop.table_name}),
            .CreateCTE => |cte| try std.fmt.allocPrint(allocator, "CREATE CTE {s}", .{cte.name}),
            .Pragma => |pragma| try std.fmt.allocPrint(allocator, "PRAGMA {s}", .{pragma.name}),
            .Explain => try allocator.dupe(u8, "EXPLAIN"),
        };
    }
};

/// Result of query execution
pub const ExecutionResult = struct {
    rows: std.ArrayList(storage.Row),
    affected_rows: u32,
    connection: *db.Connection, // Store connection to access consistent allocator

    pub fn deinit(self: *ExecutionResult) void {
        const allocator = self.connection.allocator;
        // Cleaning up execution result
        for (self.rows.items) |row| {
            // Cleaning row values
            for (row.values) |value| {
                switch (value) {
                    .Text => {
                        // Freeing text value
                    },
                    .Integer => {
                        // Freeing integer value
                    },
                    else => {
                        // Freeing other value
                    },
                }
                value.deinit(allocator);
            }
            // Freeing values array
            allocator.free(row.values);
        }
        self.rows.deinit(allocator);
        // Cleanup completed
    }
};

/// SQL LIKE pattern matching with % and _ wildcards
/// % matches zero or more characters
/// _ matches exactly one character
fn likeMatch(text: []const u8, pattern: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len) {
            const pc = pattern[pi];
            if (pc == '%') {
                // Remember position for backtracking
                star_pi = pi;
                star_ti = ti;
                pi += 1;
                continue;
            } else if (pc == '_' or std.ascii.toLower(pc) == std.ascii.toLower(text[ti])) {
                // Match single character or exact match (case-insensitive)
                ti += 1;
                pi += 1;
                continue;
            }
        }
        // No match - try backtracking to last %
        if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Skip trailing % in pattern
    while (pi < pattern.len and pattern[pi] == '%') {
        pi += 1;
    }

    return pi == pattern.len;
}

/// VM errors
const VmError = error{
    ColumnValueMismatch,
    ColumnNotFound,
    TooManyValues,
    MissingRequiredValue,
    ParameterIndexOutOfBounds,
    NoParametersProvided,
    UnresolvedParameter,
    TableNotFound,
    UnexpectedAggregate,
    NotImplemented,
};

/// Execute a parsed statement (convenience function)
pub fn execute(connection: *db.Connection, parsed: *const ast.Statement) !void {
    var vm = VirtualMachine.init(connection.allocator, connection);

    var query_planner = planner.Planner.init(connection.allocator);
    var plan = try query_planner.plan(parsed);
    defer plan.deinit();

    var result = try vm.execute(&plan);
    defer result.deinit();

    // Print results based on statement type (disabled for benchmarks - verbose output)
    // Uncomment these lines for interactive/debugging use
    // switch (parsed.*) {
    //     .Select => {
    //         std.debug.print("┌─ Query Results ─┐\n", .{});
    //         if (result.rows.items.len == 0) {
    //             std.debug.print("│ No rows found   │\n", .{});
    //         } else {
    //             for (result.rows.items, 0..) |row, i| {
    //                 std.debug.print("│ Row {d}: ", .{i + 1});
    //                 for (row.values, 0..) |value, j| {
    //                     if (j > 0) std.debug.print(", ", .{});
    //                     switch (value) {
    //                         .Integer => |int| std.debug.print("{d}", .{int}),
    //                         .Text => |text| std.debug.print("'{s}'", .{text}),
    //                         .Real => |real| std.debug.print("{d:.2}", .{real}),
    //                         .Null => std.debug.print("NULL", .{}),
    //                         .Blob => std.debug.print("<blob>", .{}),
    //                         .Parameter => |param_index| std.debug.print("?{d}", .{param_index}),
    //                         .FunctionCall => std.debug.print("<function>", .{}),
    //                         .JSON => |json| std.debug.print("JSON:'{s}'", .{json}),
    //                         .JSONB => |jsonb| std.debug.print("JSONB:'{s}'", .{jsonb.toString(connection.allocator) catch "invalid"}),
    //                         .UUID => |uuid| std.debug.print("UUID:{any}", .{uuid}),
    //                         .Array => |array| std.debug.print("ARRAY:{s}", .{array.toString(connection.allocator) catch "invalid"}),
    //                         .Boolean => |b| std.debug.print("{}", .{b}),
    //                         .Timestamp => |ts| std.debug.print("TS:{d}", .{ts}),
    //                         .TimestampTZ => |tstz| std.debug.print("TSTZ:{d}({s})", .{ tstz.timestamp, tstz.timezone }),
    //                         .Date => |d| std.debug.print("DATE:{d}", .{d}),
    //                         .Time => |t| std.debug.print("TIME:{d}", .{t}),
    //                         .Interval => |interval| std.debug.print("INTERVAL:{d}", .{interval}),
    //                         .Numeric => |n| std.debug.print("NUMERIC:{s}", .{n.digits}),
    //                         .SmallInt => |si| std.debug.print("{d}", .{si}),
    //                         .BigInt => |bi| std.debug.print("{d}", .{bi}),
    //                     }
    //                 }
    //                 std.debug.print(" │\n", .{});
    //             }
    //         }
    //         std.debug.print("└─ {d} row(s) ─────┘\n", .{result.rows.items.len});
    //     },
    //     else => {
    //         std.debug.print("✅ Statement executed successfully. Affected rows: {d}\n", .{result.affected_rows});
    //     },
    // }
}

test "vm creation" {
    try std.testing.expect(true); // Placeholder
}
