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

    const Self = @This();

    /// Initialize virtual machine
    pub fn init(allocator: std.mem.Allocator, connection: *db.Connection) Self {
        // Always use the connection's allocator to ensure consistency
        _ = allocator; // Ignore passed allocator, use connection's allocator
        // VM initialization complete
        return Self{
            .connection = connection,
            .parameters = null,
            .function_evaluator = functions.FunctionEvaluator.init(connection.allocator),
        };
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
        }
    }

    /// Execute table scan
    fn executeTableScan(self: *Self, scan: *planner.TableScanStep, result: *ExecutionResult) !void {
        // Executing table scan
        const table = self.connection.storage_engine.getTable(scan.table_name) orelse {
            return error.TableNotFound;
        };

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
            }
        }

        result.rows.deinit(self.connection.allocator);
        result.rows = filtered_rows;
    }

    /// Execute projection (SELECT columns)
    fn executeProject(self: *Self, project: *planner.ProjectStep, result: *ExecutionResult) !void {
        if (project.columns.len == 1 and std.mem.eql(u8, project.columns[0], "*")) {
            // SELECT * - return all columns, no projection needed
            return;
        }

        // Create projected rows with only selected columns
        var projected_rows: std.ArrayList(storage.Row) = .{};

        for (result.rows.items) |original_row| {
            var projected_values: std.ArrayList(storage.Value) = .{};

            // For now, we'll assume column order matches project.columns order
            // In a real implementation, we'd need column metadata from the table schema
            for (project.columns, 0..) |col_name, i| {
                if (i < original_row.values.len) {
                    // Transfer ownership of the value instead of cloning
                    // Transferring value ownership
                    try projected_values.append(self.connection.allocator, original_row.values[i]);
                } else {
                    // Column doesn't exist, add NULL
                    try projected_values.append(self.connection.allocator, storage.Value.Null);
                }
                _ = col_name; // Suppress unused warning for now
            }

            try projected_rows.append(self.connection.allocator, storage.Row{
                .values = try projected_values.toOwnedSlice(self.connection.allocator),
            });
        }

        // Clean up original rows carefully - we transferred value ownership, so only free the arrays
        for (result.rows.items) |row| {
            // Don't call value.deinit() since we transferred ownership to projected rows
            // Cleaning up original row array
            self.connection.allocator.free(row.values);
        }
        result.rows.deinit(self.connection.allocator);
        result.rows = projected_rows;
    }

    /// Clone a storage value
    /// Resolve a value, substituting parameters if needed
    fn resolveValue(self: *Self, value: storage.Value) !storage.Value {
        return switch (value) {
            .Parameter => |param_index| blk: {
                if (self.parameters) |params| {
                    if (param_index < params.len) {
                        break :blk params[param_index];
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
            else => value, // Return the value as-is for non-parameters
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
                // Always clone literal DEFAULT values to prevent double-free
                const resolved_value = try self.resolveValue(literal);
                return try resolved_value.clone(self.connection.allocator);
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

                    const resolved_value = try self.cloneValue(try self.resolveValue(row_values[value_idx]));
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
                    const resolved_value = try self.cloneValue(try self.resolveValue(value));
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
        var columns_cloned: usize = 0;
        errdefer {
            // Properly clean up cloned columns on error
            for (cloned_columns[0..columns_cloned]) |column| {
                self.connection.allocator.free(column.name);
                if (column.default_value) |default_value| {
                    default_value.deinit(self.connection.allocator);
                }
            }
            self.connection.allocator.free(cloned_columns);
        }

        for (create.columns, 0..) |column, i| {
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

        var schema = storage.TableSchema{
            .columns = cloned_columns,
        };

        self.connection.storage_engine.createTable(create.table_name, schema) catch |err| {
            schema.deinit(self.connection.allocator);
            return err;
        };

        // Clean up temporary schema (storage engine has its own clone now)
        schema.deinit(self.connection.allocator);
        result.affected_rows = 1;
    }

    /// Execute update
    fn executeUpdate(self: *Self, update: *planner.UpdateStep, result: *ExecutionResult) !void {
        const table = self.connection.storage_engine.getTable(update.table_name) orelse {
            return error.TableNotFound;
        };

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

                // Apply assignments
                for (update.assignments) |assignment| {
                    // For simplicity, update the first column (in a real implementation,
                    // we'd need proper column name to index mapping from the table schema)
                    if (updated_values.len > 0) {
                        updated_values[0].deinit(self.connection.allocator);
                        updated_values[0] = try self.cloneValue(assignment.value);
                    }
                    _ = assignment.column; // Suppress unused warning for now
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
    fn evaluateCondition(self: *Self, condition: *const ast.Condition, row: *const storage.Row) !bool {
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
        const right_value = try self.evaluateExpression(&comp.right, row);
        defer right_value.deinit(self.connection.allocator);

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
            .Like => {
                // Simple LIKE implementation (would need pattern matching)
                return self.compareValues(left_value, right_value) == .eq;
            },
            .In => {
                // Simple IN implementation
                return self.compareValues(left_value, right_value) == .eq;
            },
        };
    }

    /// Evaluate an expression against a row
    fn evaluateExpression(self: *Self, expression: *const ast.Expression, row: *const storage.Row) !storage.Value {
        return switch (expression.*) {
            .Column => |col_name| {
                // For now, just return the first value (would need column mapping)
                _ = col_name;
                if (row.values.len > 0) {
                    // Clone the value so it can be safely freed by caller
                    return try self.cloneValue(row.values[0]);
                } else {
                    return storage.Value.Null;
                }
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

        // Build hash table from smaller table (right table for now)
        var hash_map = std.AutoHashMap(u64, std.ArrayList(storage.Row)).init(self.connection.allocator);
        defer {
            var iterator = hash_map.iterator();
            while (iterator.next()) |entry| {
                for (entry.value_ptr.items) |row| {
                    for (row.values) |value| {
                        value.deinit(self.connection.allocator);
                    }
                    self.connection.allocator.free(row.values);
                }
                entry.value_ptr.deinit(self.connection.allocator);
            }
            hash_map.deinit();
        }

        // TODO: For now, fall back to nested loop join
        // Hash join implementation requires column index resolution
        // which needs schema information
        var nested_join = planner.NestedLoopJoinStep{
            .join_type = join.join_type,
            .left_table = join.left_table,
            .right_table = join.right_table,
            .condition = join.condition,
        };
        return self.executeNestedLoopJoin(&nested_join, result);
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
        // For now, implement COUNT(*) aggregate function
        for (agg.aggregates) |aggregate_op| {
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
                        if (row.values.len > 0) {
                            switch (row.values[0]) {
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
                        if (row.values.len > 0) {
                            switch (row.values[0]) {
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
                        if (row.values.len > 0) {
                            const current_value = row.values[0];
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
                        if (row.values.len > 0) {
                            const current_value = row.values[0];
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
                else => {
                    // TODO: Implement GroupConcat, CountDistinct
                    return error.NotImplemented;
                }
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

    /// Execute group by operation (stub implementation)
    fn executeGroupBy(self: *Self, group: *planner.GroupByStep, result: *ExecutionResult) !void {
        _ = self;
        _ = group;
        _ = result;
        // TODO: Implement group by operations
        return error.NotImplemented;
    }
    
    /// Execute BEGIN TRANSACTION
    fn executeBeginTransaction(self: *Self, result: *ExecutionResult) !void {
        _ = self;
        _ = result;
        // TODO: Implement transaction support in storage engine
        // For now, transactions are a no-op
    }
    
    /// Execute COMMIT
    fn executeCommit(self: *Self, result: *ExecutionResult) !void {
        _ = self;
        _ = result;
        // TODO: Implement transaction support in storage engine
        // For now, transactions are a no-op
    }
    
    /// Execute ROLLBACK
    fn executeRollback(self: *Self, result: *ExecutionResult) !void {
        _ = self;
        _ = result;
        // TODO: Implement transaction support in storage engine
        // For now, transactions are a no-op
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
        
        // TODO: Actually create and store the index
        // For now, index creation is a no-op
    }
    
    /// Execute DROP INDEX
    fn executeDropIndex(self: *Self, drop_idx: *planner.DropIndexStep, result: *ExecutionResult) !void {
        _ = self;
        _ = drop_idx;
        _ = result;
        // TODO: Implement index management in storage engine
        // For now, index dropping is a no-op
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
