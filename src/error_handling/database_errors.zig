const std = @import("std");

/// Enhanced database error system with detailed context
pub const DatabaseError = union(enum) {
    // SQL Parsing Errors
    SyntaxError: SyntaxError,
    SemanticError: SemanticError,

    // Storage Errors
    StorageError: StorageError,

    // Connection Errors
    ConnectionError: ConnectionError,

    // Transaction Errors
    TransactionError: TransactionError,

    // Constraint Errors
    ConstraintError: ConstraintError,

    // Type Errors
    TypeError: TypeError,

    // System Errors
    SystemError: SystemError,

    pub fn format(self: DatabaseError, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        switch (self) {
            .SyntaxError => |err| try writer.print("Syntax Error: {s} at line {d}, column {d}", .{ err.message, err.line, err.column }),
            .SemanticError => |err| try writer.print("Semantic Error: {s} in {s}", .{ err.message, err.context }),
            .StorageError => |err| try writer.print("Storage Error: {s} (code: {d})", .{ err.message, err.error_code }),
            .ConnectionError => |err| try writer.print("Connection Error: {s}", .{err.message}),
            .TransactionError => |err| try writer.print("Transaction Error: {s} (state: {s})", .{ err.message, @tagName(err.state) }),
            .ConstraintError => |err| try writer.print("Constraint Violation: {s} on {s}", .{ err.message, err.constraint_name }),
            .TypeError => |err| try writer.print("Type Error: {s} (expected {s}, got {s})", .{ err.message, @tagName(err.expected_type), @tagName(err.actual_type) }),
            .SystemError => |err| try writer.print("System Error: {s} (errno: {d})", .{ err.message, err.errno }),
        }
    }

    pub fn deinit(self: DatabaseError, allocator: std.mem.Allocator) void {
        switch (self) {
            .SyntaxError => |err| err.deinit(allocator),
            .SemanticError => |err| err.deinit(allocator),
            .StorageError => |err| err.deinit(allocator),
            .ConnectionError => |err| err.deinit(allocator),
            .TransactionError => |err| err.deinit(allocator),
            .ConstraintError => |err| err.deinit(allocator),
            .TypeError => |err| err.deinit(allocator),
            .SystemError => |err| err.deinit(allocator),
        }
    }
};

/// SQL syntax error with location information
pub const SyntaxError = struct {
    message: []const u8,
    line: u32,
    column: u32,
    sql_snippet: ?[]const u8,
    suggestion: ?[]const u8,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, line: u32, column: u32, sql_snippet: ?[]const u8, suggestion: ?[]const u8) !SyntaxError {
        return SyntaxError{
            .message = try allocator.dupe(u8, message),
            .line = line,
            .column = column,
            .sql_snippet = if (sql_snippet) |snippet| try allocator.dupe(u8, snippet) else null,
            .suggestion = if (suggestion) |sugg| try allocator.dupe(u8, sugg) else null,
        };
    }

    pub fn deinit(self: SyntaxError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        if (self.sql_snippet) |snippet| allocator.free(snippet);
        if (self.suggestion) |sugg| allocator.free(sugg);
    }
};

/// Semantic error (valid syntax but invalid semantics)
pub const SemanticError = struct {
    message: []const u8,
    context: []const u8, // e.g., "table 'users'", "column 'id'"
    suggestion: ?[]const u8,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, context: []const u8, suggestion: ?[]const u8) !SemanticError {
        return SemanticError{
            .message = try allocator.dupe(u8, message),
            .context = try allocator.dupe(u8, context),
            .suggestion = if (suggestion) |sugg| try allocator.dupe(u8, sugg) else null,
        };
    }

    pub fn deinit(self: SemanticError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        allocator.free(self.context);
        if (self.suggestion) |sugg| allocator.free(sugg);
    }
};

/// Storage-related error
pub const StorageError = struct {
    message: []const u8,
    error_code: u32,
    file_path: ?[]const u8,
    operation: StorageOperation,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, error_code: u32, file_path: ?[]const u8, operation: StorageOperation) !StorageError {
        return StorageError{
            .message = try allocator.dupe(u8, message),
            .error_code = error_code,
            .file_path = if (file_path) |path| try allocator.dupe(u8, path) else null,
            .operation = operation,
        };
    }

    pub fn deinit(self: StorageError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        if (self.file_path) |path| allocator.free(path);
    }
};

pub const StorageOperation = enum {
    Read,
    Write,
    Create,
    Delete,
    Lock,
    Sync,
};

/// Connection-related error
pub const ConnectionError = struct {
    message: []const u8,
    connection_string: ?[]const u8,
    error_type: ConnectionErrorType,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, connection_string: ?[]const u8, error_type: ConnectionErrorType) !ConnectionError {
        return ConnectionError{
            .message = try allocator.dupe(u8, message),
            .connection_string = if (connection_string) |cs| try allocator.dupe(u8, cs) else null,
            .error_type = error_type,
        };
    }

    pub fn deinit(self: ConnectionError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        if (self.connection_string) |cs| allocator.free(cs);
    }
};

pub const ConnectionErrorType = enum {
    Timeout,
    Refused,
    PoolExhausted,
    InvalidCredentials,
    DatabaseNotFound,
};

/// Transaction-related error
pub const TransactionError = struct {
    message: []const u8,
    state: TransactionState,
    isolation_level: ?IsolationLevel,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, state: TransactionState, isolation_level: ?IsolationLevel) !TransactionError {
        return TransactionError{
            .message = try allocator.dupe(u8, message),
            .state = state,
            .isolation_level = isolation_level,
        };
    }

    pub fn deinit(self: TransactionError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
    }
};

pub const TransactionState = enum {
    NotStarted,
    Active,
    Committed,
    RolledBack,
    Failed,
};

pub const IsolationLevel = enum {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
};

/// Constraint violation error
pub const ConstraintError = struct {
    message: []const u8,
    constraint_name: []const u8,
    constraint_type: ConstraintType,
    table_name: ?[]const u8,
    column_name: ?[]const u8,
    conflicting_value: ?[]const u8,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, constraint_name: []const u8, constraint_type: ConstraintType, table_name: ?[]const u8, column_name: ?[]const u8, conflicting_value: ?[]const u8) !ConstraintError {
        return ConstraintError{
            .message = try allocator.dupe(u8, message),
            .constraint_name = try allocator.dupe(u8, constraint_name),
            .constraint_type = constraint_type,
            .table_name = if (table_name) |tn| try allocator.dupe(u8, tn) else null,
            .column_name = if (column_name) |cn| try allocator.dupe(u8, cn) else null,
            .conflicting_value = if (conflicting_value) |cv| try allocator.dupe(u8, cv) else null,
        };
    }

    pub fn deinit(self: ConstraintError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        allocator.free(self.constraint_name);
        if (self.table_name) |tn| allocator.free(tn);
        if (self.column_name) |cn| allocator.free(cn);
        if (self.conflicting_value) |cv| allocator.free(cv);
    }
};

pub const ConstraintType = enum {
    PrimaryKey,
    ForeignKey,
    Unique,
    NotNull,
    Check,
};

/// Type mismatch error
pub const TypeError = struct {
    message: []const u8,
    expected_type: DataType,
    actual_type: DataType,
    context: ?[]const u8,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, expected_type: DataType, actual_type: DataType, context: ?[]const u8) !TypeError {
        return TypeError{
            .message = try allocator.dupe(u8, message),
            .expected_type = expected_type,
            .actual_type = actual_type,
            .context = if (context) |ctx| try allocator.dupe(u8, ctx) else null,
        };
    }

    pub fn deinit(self: TypeError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        if (self.context) |ctx| allocator.free(ctx);
    }
};

pub const DataType = enum {
    Integer,
    Text,
    Real,
    Blob,
    JSON,
    JSONB,
    UUID,
    Array,
    Boolean,
    Timestamp,
    Date,
    Time,
    Numeric,
    Unknown,
};

/// System-level error
pub const SystemError = struct {
    message: []const u8,
    errno: i32,
    operation: []const u8,

    pub fn init(allocator: std.mem.Allocator, message: []const u8, errno: i32, operation: []const u8) !SystemError {
        return SystemError{
            .message = try allocator.dupe(u8, message),
            .errno = errno,
            .operation = try allocator.dupe(u8, operation),
        };
    }

    pub fn deinit(self: SystemError, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        allocator.free(self.operation);
    }
};

/// Error builder for convenient error creation
pub const ErrorBuilder = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ErrorBuilder {
        return ErrorBuilder{ .allocator = allocator };
    }

    pub fn syntaxError(self: ErrorBuilder, message: []const u8, line: u32, column: u32) DatabaseError {
        const err = SyntaxError.init(self.allocator, message, line, column, null, null) catch |e| {
            std.debug.panic("Failed to create syntax error: {}", .{e});
        };
        return DatabaseError{ .SyntaxError = err };
    }

    pub fn syntaxErrorWithSuggestion(self: ErrorBuilder, message: []const u8, line: u32, column: u32, suggestion: []const u8) DatabaseError {
        const err = SyntaxError.init(self.allocator, message, line, column, null, suggestion) catch |e| {
            std.debug.panic("Failed to create syntax error: {}", .{e});
        };
        return DatabaseError{ .SyntaxError = err };
    }

    pub fn semanticError(self: ErrorBuilder, message: []const u8, context: []const u8) DatabaseError {
        const err = SemanticError.init(self.allocator, message, context, null) catch |e| {
            std.debug.panic("Failed to create semantic error: {}", .{e});
        };
        return DatabaseError{ .SemanticError = err };
    }

    pub fn constraintError(self: ErrorBuilder, message: []const u8, constraint_name: []const u8, constraint_type: ConstraintType) DatabaseError {
        const err = ConstraintError.init(self.allocator, message, constraint_name, constraint_type, null, null, null) catch |e| {
            std.debug.panic("Failed to create constraint error: {}", .{e});
        };
        return DatabaseError{ .ConstraintError = err };
    }

    pub fn typeError(self: ErrorBuilder, message: []const u8, expected_type: DataType, actual_type: DataType) DatabaseError {
        const err = TypeError.init(self.allocator, message, expected_type, actual_type, null) catch |e| {
            std.debug.panic("Failed to create type error: {}", .{e});
        };
        return DatabaseError{ .TypeError = err };
    }
};

/// Error formatter for pretty-printing errors
pub const ErrorFormatter = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ErrorFormatter {
        return ErrorFormatter{ .allocator = allocator };
    }

    /// Format error with context and suggestions
    pub fn formatDetailed(self: ErrorFormatter, err: DatabaseError, sql: ?[]const u8) ![]u8 {
        var output = std.ArrayList(u8){};
        defer output.deinit();

        const writer = output.writer();

        // Write main error message
        try writer.print("ERROR: {}\n", .{err});

        // Add context if available
        switch (err) {
            .SyntaxError => |syntax_err| {
                if (sql) |query| {
                    try self.formatSqlContext(writer, query, syntax_err.line, syntax_err.column);
                }
                if (syntax_err.suggestion) |suggestion| {
                    try writer.print("SUGGESTION: {s}\n", .{suggestion});
                }
            },
            .SemanticError => |semantic_err| {
                if (semantic_err.suggestion) |suggestion| {
                    try writer.print("SUGGESTION: {s}\n", .{suggestion});
                }
            },
            else => {},
        }

        return try output.toOwnedSlice();
    }

    /// Format SQL context with error highlighting
    fn formatSqlContext(self: ErrorFormatter, writer: anytype, sql: []const u8, error_line: u32, error_column: u32) !void {
        _ = self;

        const lines = std.mem.split(u8, sql, "\n");
        var current_line: u32 = 1;
        var line_iter = lines;

        try writer.print("\nSQL:\n");
        while (line_iter.next()) |line| {
            if (current_line >= error_line - 2 and current_line <= error_line + 2) {
                const prefix = if (current_line == error_line) ">>> " else "    ";
                try writer.print("{s}{d}: {s}\n", .{ prefix, current_line, line });

                if (current_line == error_line) {
                    // Add error indicator
                    try writer.print("    ");
                    for (0..error_column + 3) |_| { // +3 for line number and ": "
                        try writer.print(" ");
                    }
                    try writer.print("^\n");
                }
            }
            current_line += 1;
        }
        try writer.print("\n");
    }
};

test "error creation and formatting" {
    const allocator = std.testing.allocator;

    const builder = ErrorBuilder.init(allocator);
    var err = builder.syntaxErrorWithSuggestion("Unexpected token", 5, 12, "Did you mean 'SELECT'?");
    defer err.deinit(allocator);

    const formatter = ErrorFormatter.init(allocator);
    const formatted = try formatter.formatDetailed(err, "SELECT * FROM users\nWHER name = 'John';");
    defer allocator.free(formatted);

    try std.testing.expect(std.mem.indexOf(u8, formatted, "Unexpected token") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "Did you mean 'SELECT'?") != null);
}
