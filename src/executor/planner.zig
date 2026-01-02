const std = @import("std");
const ast = @import("../parser/ast.zig");
const storage = @import("../db/storage.zig");

/// Query execution planner
pub const Planner = struct {
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initialize planner
    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    /// Create execution plan for a statement
    pub fn plan(self: *Self, statement: *const ast.Statement) !ExecutionPlan {
        return switch (statement.*) {
            .Select => |*select| try self.planSelect(select),
            .Insert => |*insert| try self.planInsert(insert),
            .CreateTable => |*create| try self.planCreateTable(create),
            .Update => |*update| try self.planUpdate(update),
            .Delete => |*delete| try self.planDelete(delete),
            .BeginTransaction => |*trans| try self.planTransaction(trans),
            .Commit => |*trans| try self.planCommit(trans),
            .Rollback => |*trans| try self.planRollback(trans),
            .CreateIndex => |*create_idx| try self.planCreateIndex(create_idx),
            .DropIndex => |*drop_idx| try self.planDropIndex(drop_idx),
            .DropTable => |*drop_tbl| try self.planDropTable(drop_tbl),
            .With => |*with| try self.planWith(with), // Handle CTE
        };
    }

    /// Plan SELECT statement execution
    fn planSelect(self: *Self, select: *const ast.SelectStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        // Table scan step
        try steps.append(self.allocator, ExecutionStep{
            .TableScan = TableScanStep{
                .table_name = if (select.table) |table| try self.allocator.dupe(u8, table) else "",
            },
        });

        // JOIN steps
        for (select.joins) |join| {
            const join_step = try self.planJoin(select.table orelse "", &join);
            try steps.append(self.allocator, join_step);
        }

        // Filter step (WHERE clause)
        if (select.where_clause) |where_clause| {
            try steps.append(self.allocator, ExecutionStep{
                .Filter = FilterStep{
                    .condition = try self.cloneCondition(&where_clause.condition),
                },
            });
        }

        // Check if we have aggregate functions
        const has_aggregates = self.hasAggregates(select.columns);

        if (has_aggregates) {
            // Extract aggregate operations
            var aggregates: std.ArrayList(AggregateOperation) = .{};
            for (select.columns) |column| {
                if (column.expression == .Aggregate) {
                    try aggregates.append(self.allocator, AggregateOperation{
                        .function_type = column.expression.Aggregate.function_type,
                        .column = if (column.expression.Aggregate.column) |col|
                            try self.allocator.dupe(u8, col)
                        else
                            null,
                        .alias = if (column.alias) |alias|
                            try self.allocator.dupe(u8, alias)
                        else
                            null,
                    });
                }
            }

            if (select.group_by) |group_by| {
                // GROUP BY aggregation
                var group_columns: std.ArrayList([]const u8) = .{};
                for (group_by) |col| {
                    try group_columns.append(self.allocator, try self.allocator.dupe(u8, col));
                }

                try steps.append(self.allocator, ExecutionStep{
                    .GroupBy = GroupByStep{
                        .group_columns = try group_columns.toOwnedSlice(self.allocator),
                        .aggregates = try aggregates.toOwnedSlice(self.allocator),
                    },
                });
            } else {
                // Simple aggregation (no GROUP BY)
                try steps.append(self.allocator, ExecutionStep{
                    .Aggregate = AggregateStep{
                        .aggregates = try aggregates.toOwnedSlice(self.allocator),
                    },
                });
            }
        } else {
            // Regular projection step (SELECT columns)
            var columns: std.ArrayList([]const u8) = .{};
            for (select.columns) |column| {
                switch (column.expression) {
                    .Simple => |name| try columns.append(self.allocator, try self.allocator.dupe(u8, name)),
                    .Aggregate => {
                        // This shouldn't happen if has_aggregates was false
                        return error.UnexpectedAggregate;
                    },
                    .Window => {
                        // Window functions will be handled in a later version
                        try columns.append(self.allocator, try self.allocator.dupe(u8, "window_result"));
                    },
                    .FunctionCall => {
                        // Function calls will be handled in a later version
                        try columns.append(self.allocator, try self.allocator.dupe(u8, "function_result"));
                    },
                }
            }

            try steps.append(self.allocator, ExecutionStep{
                .Project = ProjectStep{
                    .columns = try columns.toOwnedSlice(self.allocator),
                },
            });
        }

        // Limit step
        if (select.limit) |limit| {
            try steps.append(self.allocator, ExecutionStep{
                .Limit = LimitStep{
                    .count = limit,
                    .offset = select.offset orelse 0,
                },
            });
        }

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan JOIN operation
    fn planJoin(self: *Self, left_table: []const u8, join: *const ast.JoinClause) !ExecutionStep {
        // Try to determine if this is an equi-join for hash join optimization
        const equi_join_info = self.analyzeEquiJoin(&join.condition);

        if (equi_join_info) |info| {
            // Use hash join for equi-joins (more efficient for larger datasets)
            return ExecutionStep{
                .HashJoin = HashJoinStep{
                    .join_type = join.join_type,
                    .left_table = try self.allocator.dupe(u8, left_table),
                    .right_table = try self.allocator.dupe(u8, join.table),
                    .left_key_column = try self.allocator.dupe(u8, info.left_column),
                    .right_key_column = try self.allocator.dupe(u8, info.right_column),
                    .condition = try self.cloneCondition(&join.condition),
                },
            };
        } else {
            // Use nested loop join for complex conditions
            return ExecutionStep{
                .NestedLoopJoin = NestedLoopJoinStep{
                    .join_type = join.join_type,
                    .left_table = try self.allocator.dupe(u8, left_table),
                    .right_table = try self.allocator.dupe(u8, join.table),
                    .condition = try self.cloneCondition(&join.condition),
                },
            };
        }
    }

    /// Analyze if condition is an equi-join (column = column)
    fn analyzeEquiJoin(self: *Self, condition: *const ast.Condition) ?EquiJoinInfo {
        _ = self;
        switch (condition.*) {
            .Comparison => |comp| {
                if (comp.operator == .Equal) {
                    // Check if both sides are column references
                    if (comp.left == .Column and comp.right == .Column) {
                        return EquiJoinInfo{
                            .left_column = comp.left.Column,
                            .right_column = comp.right.Column,
                        };
                    }
                }
            },
            .Logical => {
                // For now, don't optimize complex logical conditions
                // Could be enhanced to handle AND of equi-joins
            },
        }
        return null;
    }

    const EquiJoinInfo = struct {
        left_column: []const u8,
        right_column: []const u8,
    };

    /// Check if any columns contain aggregate functions
    fn hasAggregates(self: *Self, columns: []ast.Column) bool {
        _ = self;
        for (columns) |column| {
            if (column.expression == .Aggregate) {
                return true;
            }
        }
        return false;
    }

    /// Plan INSERT statement execution
    fn planInsert(self: *Self, insert: *const ast.InsertStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        // Clone columns if provided
        var columns: ?[][]const u8 = null;
        if (insert.columns) |cols| {
            var cloned_cols: std.ArrayList([]const u8) = .{};
            for (cols) |col| {
                try cloned_cols.append(self.allocator, try self.allocator.dupe(u8, col));
            }
            columns = try cloned_cols.toOwnedSlice(self.allocator);
        }

        // Clone values
        var values: std.ArrayList([]storage.Value) = .{};
        for (insert.values) |row| {
            var cloned_row: std.ArrayList(storage.Value) = .{};
            for (row) |value| {
                try cloned_row.append(self.allocator, try self.cloneValue(value));
            }
            try values.append(self.allocator, try cloned_row.toOwnedSlice(self.allocator));
        }

        try steps.append(self.allocator, ExecutionStep{
            .Insert = InsertStep{
                .table_name = try self.allocator.dupe(u8, insert.table),
                .columns = columns,
                .values = try values.toOwnedSlice(self.allocator),
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan CREATE TABLE statement execution
    fn planCreateTable(self: *Self, create: *const ast.CreateTableStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        // Clone column definitions
        var columns: std.ArrayList(storage.Column) = .{};
        for (create.columns) |col_def| {
            try columns.append(self.allocator, storage.Column{
                .name = try self.allocator.dupe(u8, col_def.name),
                .data_type = switch (col_def.data_type) {
                    .Integer => storage.DataType.Integer,
                    .Text => storage.DataType.Text,
                    .Real => storage.DataType.Real,
                    .Blob => storage.DataType.Blob,
                    // Map extended types to their storage equivalents
                    .DateTime => storage.DataType.Text, // Store as ISO string
                    .Timestamp => storage.DataType.Integer, // Store as Unix timestamp
                    .Boolean => storage.DataType.Integer, // Store as 0/1
                    .Date => storage.DataType.Text, // Store as ISO date
                    .Time => storage.DataType.Text, // Store as ISO time
                    .Decimal => storage.DataType.Real, // Store as float
                    .Varchar => storage.DataType.Text,
                    .Char => storage.DataType.Text,
                    .Float => storage.DataType.Real,
                    .Double => storage.DataType.Real,
                    .SmallInt => storage.DataType.Integer,
                    .BigInt => storage.DataType.Integer,
                    // PostgreSQL compatibility types
                    .JSON => storage.DataType.JSON,
                    .JSONB => storage.DataType.JSONB,
                    .UUID => storage.DataType.UUID,
                    .Array => storage.DataType.Array,
                    .TimestampTZ => storage.DataType.TimestampTZ,
                    .Interval => storage.DataType.Interval,
                    .Numeric => storage.DataType.Numeric,
                },
                .is_primary_key = blk: {
                    for (col_def.constraints) |constraint| {
                        if (constraint == .PrimaryKey) break :blk true;
                    }
                    break :blk false;
                },
                .is_nullable = blk: {
                    for (col_def.constraints) |constraint| {
                        if (constraint == .NotNull) break :blk false;
                    }
                    break :blk true;
                },
                .default_value = blk: {
                    for (col_def.constraints) |constraint| {
                        if (constraint == .Default) {
                            const default_value = try self.convertAstDefaultToStorage(constraint.Default);
                            break :blk default_value;
                        }
                    }
                    break :blk null;
                },
            });
        }

        try steps.append(self.allocator, ExecutionStep{
            .CreateTable = CreateTableStep{
                .table_name = try self.allocator.dupe(u8, create.table_name),
                .columns = try columns.toOwnedSlice(self.allocator),
                .if_not_exists = create.if_not_exists,
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan UPDATE statement execution
    fn planUpdate(self: *Self, update: *const ast.UpdateStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        // Clone assignments
        var assignments: std.ArrayList(UpdateAssignment) = .{};
        for (update.assignments) |assignment| {
            try assignments.append(self.allocator, UpdateAssignment{
                .column = try self.allocator.dupe(u8, assignment.column),
                .value = try self.cloneValue(assignment.value),
            });
        }

        var condition: ?ast.Condition = null;
        if (update.where_clause) |where_clause| {
            condition = try self.cloneCondition(&where_clause.condition);
        }

        try steps.append(self.allocator, ExecutionStep{
            .Update = UpdateStep{
                .table_name = try self.allocator.dupe(u8, update.table),
                .assignments = try assignments.toOwnedSlice(self.allocator),
                .condition = condition,
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan DELETE statement execution
    fn planDelete(self: *Self, delete: *const ast.DeleteStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        var condition: ?ast.Condition = null;
        if (delete.where_clause) |where_clause| {
            condition = try self.cloneCondition(&where_clause.condition);
        }

        try steps.append(self.allocator, ExecutionStep{
            .Delete = DeleteStep{
                .table_name = try self.allocator.dupe(u8, delete.table),
                .condition = condition,
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan transaction statement
    fn planTransaction(self: *Self, trans: *const ast.TransactionStatement) !ExecutionPlan {
        _ = trans;
        var steps: std.ArrayList(ExecutionStep) = .{};

        try steps.append(self.allocator, ExecutionStep{
            .BeginTransaction = {},
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan commit statement
    fn planCommit(self: *Self, trans: *const ast.TransactionStatement) !ExecutionPlan {
        _ = trans;
        var steps: std.ArrayList(ExecutionStep) = .{};

        try steps.append(self.allocator, ExecutionStep{
            .Commit = {},
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan rollback statement
    fn planRollback(self: *Self, trans: *const ast.TransactionStatement) !ExecutionPlan {
        _ = trans;
        var steps: std.ArrayList(ExecutionStep) = .{};

        try steps.append(self.allocator, ExecutionStep{
            .Rollback = {},
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan create index statement
    fn planCreateIndex(self: *Self, create_idx: *const ast.CreateIndexStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        // Clone columns
        var columns = try self.allocator.alloc([]const u8, create_idx.columns.len);
        for (create_idx.columns, 0..) |col, i| {
            columns[i] = try self.allocator.dupe(u8, col);
        }

        try steps.append(self.allocator, ExecutionStep{
            .CreateIndex = CreateIndexStep{
                .index_name = try self.allocator.dupe(u8, create_idx.index_name),
                .table_name = try self.allocator.dupe(u8, create_idx.table_name),
                .columns = columns,
                .unique = create_idx.unique,
                .if_not_exists = create_idx.if_not_exists,
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan drop index statement
    fn planDropIndex(self: *Self, drop_idx: *const ast.DropIndexStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        try steps.append(self.allocator, ExecutionStep{
            .DropIndex = DropIndexStep{
                .index_name = try self.allocator.dupe(u8, drop_idx.index_name),
                .if_exists = drop_idx.if_exists,
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Plan drop table statement
    fn planDropTable(self: *Self, drop_tbl: *const ast.DropTableStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        try steps.append(self.allocator, ExecutionStep{
            .DropTable = DropTableStep{
                .table_name = try self.allocator.dupe(u8, drop_tbl.table_name),
                .if_exists = drop_tbl.if_exists,
            },
        });

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Clone a condition (deep copy)
    fn cloneCondition(self: *Self, condition: *const ast.Condition) !ast.Condition {
        return switch (condition.*) {
            .Comparison => |*comp| ast.Condition{
                .Comparison = ast.ComparisonCondition{
                    .left = try self.cloneExpression(&comp.left),
                    .operator = comp.operator,
                    .right = try self.cloneExpression(&comp.right),
                },
            },
            .Logical => |*logical| {
                const left_ptr = try self.allocator.create(ast.Condition);
                left_ptr.* = try self.cloneCondition(logical.left);

                const right_ptr = try self.allocator.create(ast.Condition);
                right_ptr.* = try self.cloneCondition(logical.right);

                return ast.Condition{
                    .Logical = ast.LogicalCondition{
                        .left = left_ptr,
                        .operator = logical.operator,
                        .right = right_ptr,
                    },
                };
            },
        };
    }

    /// Clone an expression
    fn cloneExpression(self: *Self, expression: *const ast.Expression) !ast.Expression {
        return switch (expression.*) {
            .Column => |col| ast.Expression{ .Column = try self.allocator.dupe(u8, col) },
            .Literal => |value| ast.Expression{ .Literal = try self.cloneAstValue(value) },
            .Parameter => |param_index| ast.Expression{ .Parameter = param_index },
        };
    }

    /// Clone a value from AST to storage
    fn cloneValue(self: *Self, value: ast.Value) !storage.Value {
        const ast_storage_value = switch (value) {
            .Integer => |i| storage.Value{ .Integer = i },
            .Text => |t| storage.Value{ .Text = t }, // Don't duplicate here, let clone handle it
            .Real => |r| storage.Value{ .Real = r },
            .Blob => |b| storage.Value{ .Blob = b }, // Don't duplicate here, let clone handle it
            .Null => storage.Value.Null,
            .Parameter => |param_index| storage.Value{ .Parameter = param_index },
            .FunctionCall => |function_call| {
                // Convert function call to storage representation as FunctionCall placeholder
                // This will be evaluated at runtime by the VM
                const storage_func = try self.convertAstFunctionToStorage(function_call);
                return storage.Value{ .FunctionCall = storage_func };
            },
        };
        return ast_storage_value.clone(self.allocator);
    }

    /// Clone a default value (preserving FunctionCall for VM evaluation)
    fn cloneDefaultValue(self: *Self, default_value: ast.DefaultValue) !ast.DefaultValue {
        return switch (default_value) {
            .Literal => |literal| ast.DefaultValue{ .Literal = try self.cloneAstValue(literal) },
            .FunctionCall => |function_call| ast.DefaultValue{ .FunctionCall = try self.cloneFunctionCall(function_call) },
        };
    }

    /// Clone a function call
    fn cloneFunctionCall(self: *Self, function_call: ast.FunctionCall) anyerror!ast.FunctionCall {
        var cloned_args = try self.allocator.alloc(ast.FunctionArgument, function_call.arguments.len);
        for (function_call.arguments, 0..) |arg, i| {
            cloned_args[i] = try self.cloneFunctionArgument(arg);
        }

        return ast.FunctionCall{
            .name = try self.allocator.dupe(u8, function_call.name),
            .arguments = cloned_args,
        };
    }

    /// Clone a function argument
    fn cloneFunctionArgument(self: *Self, arg: ast.FunctionArgument) anyerror!ast.FunctionArgument {
        return switch (arg) {
            .Literal => |literal| ast.FunctionArgument{ .Literal = try self.cloneAstValue(literal) },
            .String => |string| ast.FunctionArgument{ .String = try self.allocator.dupe(u8, string) },
            .Column => |column| ast.FunctionArgument{ .Column = try self.allocator.dupe(u8, column) },
            .Parameter => |param_index| ast.FunctionArgument{ .Parameter = param_index },
        };
    }

    /// Convert AST default value to storage default value
    fn convertAstDefaultToStorage(self: *Self, default_value: ast.DefaultValue) !storage.Column.DefaultValue {
        return switch (default_value) {
            .Literal => |literal| {
                const storage_value = try self.cloneValue(literal);
                return storage.Column.DefaultValue{ .Literal = storage_value };
            },
            .FunctionCall => |function_call| {
                const storage_func = try self.convertAstFunctionToStorage(function_call);
                return storage.Column.DefaultValue{ .FunctionCall = storage_func };
            },
        };
    }

    /// Convert AST function call to storage function call
    fn convertAstFunctionToStorage(self: *Self, function_call: ast.FunctionCall) !storage.Column.FunctionCall {
        var storage_args = try self.allocator.alloc(storage.Column.FunctionArgument, function_call.arguments.len);
        for (function_call.arguments, 0..) |arg, i| {
            storage_args[i] = try self.convertAstFunctionArgToStorage(arg);
        }

        return storage.Column.FunctionCall{
            .name = try self.allocator.dupe(u8, function_call.name),
            .arguments = storage_args,
        };
    }

    /// Convert AST function argument to storage function argument
    fn convertAstFunctionArgToStorage(self: *Self, arg: ast.FunctionArgument) anyerror!storage.Column.FunctionArgument {
        return switch (arg) {
            .Literal => |literal| {
                const storage_value = try self.cloneValue(literal);
                return storage.Column.FunctionArgument{ .Literal = storage_value };
            },
            .String => |string| {
                // Convert string to Text literal using proper clone
                const text_value = storage.Value{ .Text = string };
                const cloned_value = try text_value.clone(self.allocator);
                return storage.Column.FunctionArgument{ .Literal = cloned_value };
            },
            .Column => |column| {
                return storage.Column.FunctionArgument{ .Column = try self.allocator.dupe(u8, column) };
            },
            .Parameter => |param_index| {
                return storage.Column.FunctionArgument{ .Parameter = param_index };
            },
        };
    }

    /// Clone an AST value (different from storage value)
    fn cloneAstValue(self: *Self, value: ast.Value) anyerror!ast.Value {
        return switch (value) {
            .Integer => |i| ast.Value{ .Integer = i },
            .Text => |t| ast.Value{ .Text = try self.allocator.dupe(u8, t) },
            .Real => |r| ast.Value{ .Real = r },
            .Blob => |b| ast.Value{ .Blob = try self.allocator.dupe(u8, b) },
            .Null => ast.Value.Null,
            .Parameter => |param_index| ast.Value{ .Parameter = param_index },
            .FunctionCall => |function_call| ast.Value{ .FunctionCall = try self.cloneFunctionCall(function_call) },
        };
    }

    /// Plan Common Table Expression (WITH clause) execution
    fn planWith(self: *Self, with: *const ast.WithStatement) !ExecutionPlan {
        var steps: std.ArrayList(ExecutionStep) = .{};

        // For now, just plan the main query (CTE support will be enhanced later)
        _ = with.cte_definitions; // TODO: Implement full CTE planning

        const main_plan = try self.planSelect(&with.main_query);

        // Append main query steps
        for (main_plan.steps) |step| {
            try steps.append(self.allocator, step);
        }

        return ExecutionPlan{
            .steps = try steps.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }
};

/// Execution plan containing steps to execute
pub const ExecutionPlan = struct {
    steps: []ExecutionStep,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ExecutionPlan) void {
        for (self.steps) |*step| {
            step.deinit(self.allocator);
        }
        self.allocator.free(self.steps);
    }
};

/// Individual execution steps
pub const ExecutionStep = union(enum) {
    TableScan: TableScanStep,
    Filter: FilterStep,
    Project: ProjectStep,
    Limit: LimitStep,
    Insert: InsertStep,
    CreateTable: CreateTableStep,
    Update: UpdateStep,
    Delete: DeleteStep,
    NestedLoopJoin: NestedLoopJoinStep,
    HashJoin: HashJoinStep,
    Aggregate: AggregateStep,
    GroupBy: GroupByStep,
    BeginTransaction,
    Commit,
    Rollback,
    CreateIndex: CreateIndexStep,
    DropIndex: DropIndexStep,
    DropTable: DropTableStep,
    // CreateCTE: CreateCTEStep, // TODO: Add CTE support in future version

    pub fn deinit(self: *ExecutionStep, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .TableScan => |*step| step.deinit(allocator),
            .Filter => |*step| step.deinit(allocator),
            .Project => |*step| step.deinit(allocator),
            .Limit => {},
            .Insert => |*step| step.deinit(allocator),
            .CreateTable => |*step| step.deinit(allocator),
            .Update => |*step| step.deinit(allocator),
            .Delete => |*step| step.deinit(allocator),
            .NestedLoopJoin => |*step| step.deinit(allocator),
            .HashJoin => |*step| step.deinit(allocator),
            .Aggregate => |*step| step.deinit(allocator),
            .GroupBy => |*step| step.deinit(allocator),
            .BeginTransaction => {},
            .Commit => {},
            .Rollback => {},
            .CreateIndex => |*step| step.deinit(allocator),
            .DropIndex => |*step| step.deinit(allocator),
            .DropTable => |*step| step.deinit(allocator),
            // .CreateCTE => |*step| step.deinit(allocator), // TODO: Add CTE support
        }
    }
};

/// Table scan step
pub const TableScanStep = struct {
    table_name: []const u8,

    pub fn deinit(self: *TableScanStep, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
    }
};

/// Filter step (WHERE clause)
pub const FilterStep = struct {
    condition: ast.Condition,

    pub fn deinit(self: *FilterStep, allocator: std.mem.Allocator) void {
        self.condition.deinit(allocator);
    }
};

/// Projection step (SELECT columns)
pub const ProjectStep = struct {
    columns: [][]const u8,

    pub fn deinit(self: *ProjectStep, allocator: std.mem.Allocator) void {
        for (self.columns) |column| {
            allocator.free(column);
        }
        allocator.free(self.columns);
    }
};

/// Limit step
pub const LimitStep = struct {
    count: u32,
    offset: u32,
};

/// Insert step
pub const InsertStep = struct {
    table_name: []const u8,
    columns: ?[][]const u8,
    values: [][]storage.Value,

    pub fn deinit(self: *InsertStep, allocator: std.mem.Allocator) void {
        // Free table name
        allocator.free(self.table_name);

        // Free columns if they exist
        if (self.columns) |cols| {
            for (cols) |col| {
                allocator.free(col);
            }
            allocator.free(cols);
        }

        // Free values properly
        for (self.values) |row| {
            // Each row is an owned slice of Values
            for (row) |value| {
                value.deinit(allocator);
            }
            // Free the row array itself
            allocator.free(row);
        }
        // Free the values array
        allocator.free(self.values);
    }
};

/// Create table step
pub const CreateTableStep = struct {
    table_name: []const u8,
    columns: []storage.Column,
    if_not_exists: bool,

    pub fn deinit(self: *CreateTableStep, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        for (self.columns) |column| {
            allocator.free(column.name);
            if (column.default_value) |default_value| {
                default_value.deinit(allocator);
            }
        }
        allocator.free(self.columns);
    }
};

/// Update step
pub const UpdateStep = struct {
    table_name: []const u8,
    assignments: []UpdateAssignment,
    condition: ?ast.Condition,

    pub fn deinit(self: *UpdateStep, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        for (self.assignments) |assignment| {
            allocator.free(assignment.column);
            assignment.value.deinit(allocator);
        }
        allocator.free(self.assignments);
        if (self.condition) |*cond| {
            cond.deinit(allocator);
        }
    }
};

/// Delete step
pub const DeleteStep = struct {
    table_name: []const u8,
    condition: ?ast.Condition,

    pub fn deinit(self: *DeleteStep, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        if (self.condition) |*cond| {
            cond.deinit(allocator);
        }
    }
};

/// Create index step
pub const CreateIndexStep = struct {
    index_name: []const u8,
    table_name: []const u8,
    columns: [][]const u8,
    unique: bool,
    if_not_exists: bool,

    pub fn deinit(self: *CreateIndexStep, allocator: std.mem.Allocator) void {
        allocator.free(self.index_name);
        allocator.free(self.table_name);
        for (self.columns) |col| {
            allocator.free(col);
        }
        allocator.free(self.columns);
    }
};

/// Drop index step
pub const DropIndexStep = struct {
    index_name: []const u8,
    if_exists: bool,

    pub fn deinit(self: *DropIndexStep, allocator: std.mem.Allocator) void {
        allocator.free(self.index_name);
    }
};

/// Drop table step
pub const DropTableStep = struct {
    table_name: []const u8,
    if_exists: bool,

    pub fn deinit(self: *DropTableStep, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
    }
};

/// Update assignment
pub const UpdateAssignment = struct {
    column: []const u8,
    value: storage.Value,
};

/// Nested loop join step (for small tables or when no indexes available)
pub const NestedLoopJoinStep = struct {
    join_type: ast.JoinType,
    left_table: []const u8,
    right_table: []const u8,
    condition: ast.Condition,

    pub fn deinit(self: *NestedLoopJoinStep, allocator: std.mem.Allocator) void {
        allocator.free(self.left_table);
        allocator.free(self.right_table);
        self.condition.deinit(allocator);
    }
};

/// Hash join step (for larger tables with equi-join conditions)
pub const HashJoinStep = struct {
    join_type: ast.JoinType,
    left_table: []const u8,
    right_table: []const u8,
    left_key_column: []const u8,
    right_key_column: []const u8,
    condition: ast.Condition,

    pub fn deinit(self: *HashJoinStep, allocator: std.mem.Allocator) void {
        allocator.free(self.left_table);
        allocator.free(self.right_table);
        allocator.free(self.left_key_column);
        allocator.free(self.right_key_column);
        self.condition.deinit(allocator);
    }
};

/// Aggregate step (for aggregate functions without GROUP BY)
pub const AggregateStep = struct {
    aggregates: []AggregateOperation,

    pub fn deinit(self: *AggregateStep, allocator: std.mem.Allocator) void {
        for (self.aggregates) |*agg| {
            agg.deinit(allocator);
        }
        allocator.free(self.aggregates);
    }
};

/// Group by step (for aggregate functions with GROUP BY)
pub const GroupByStep = struct {
    group_columns: [][]const u8,
    aggregates: []AggregateOperation,

    pub fn deinit(self: *GroupByStep, allocator: std.mem.Allocator) void {
        for (self.group_columns) |col| {
            allocator.free(col);
        }
        allocator.free(self.group_columns);

        for (self.aggregates) |*agg| {
            agg.deinit(allocator);
        }
        allocator.free(self.aggregates);
    }
};

/// Aggregate operation definition
pub const AggregateOperation = struct {
    function_type: ast.AggregateFunctionType,
    column: ?[]const u8, // NULL for COUNT(*)
    alias: ?[]const u8,

    pub fn deinit(self: *AggregateOperation, allocator: std.mem.Allocator) void {
        if (self.column) |col| {
            allocator.free(col);
        }
        if (self.alias) |alias| {
            allocator.free(alias);
        }
    }
};

/// CTE creation step
pub const CreateCTEStep = struct {
    name: []const u8,
    plan: ExecutionPlan,
    recursive: bool,

    pub fn deinit(self: *CreateCTEStep, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.plan.deinit();
    }
};

test "planner creation" {
    const allocator = std.testing.allocator;
    const planner = Planner.init(allocator);
    _ = planner; // Suppress unused variable warning
    try std.testing.expect(true);
}
