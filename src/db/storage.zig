const std = @import("std");
const btree = @import("btree.zig");
const pager = @import("pager.zig");
const memory_pool = @import("memory_pool.zig");

/// Row identifier type used throughout the database
pub const RowId = u64;

/// Storage engine that manages tables and data persistence
pub const StorageEngine = struct {
    allocator: std.mem.Allocator,
    pager: *pager.Pager,
    memory_pool: memory_pool.MemoryPool,
    pooled_allocator: memory_pool.PooledAllocator,
    tables: std.StringHashMap(*Table),
    indexes: std.StringHashMap(*Index),
    is_memory: bool,

    const Self = @This();

    /// Initialize storage engine with file backing
    pub fn init(allocator: std.mem.Allocator, path: []const u8) !*Self {
        var engine = try allocator.create(Self);
        engine.allocator = allocator;
        engine.pager = try pager.Pager.init(allocator, path);
        engine.memory_pool = memory_pool.MemoryPool.init(allocator);
        engine.pooled_allocator = memory_pool.PooledAllocator.init(&engine.memory_pool);
        engine.tables = std.StringHashMap(*Table).init(allocator);
        engine.indexes = std.StringHashMap(*Index).init(allocator);
        engine.is_memory = false;

        // Load existing tables from file
        try engine.loadTables();

        return engine;
    }

    /// Initialize in-memory storage engine
    pub fn initMemory(allocator: std.mem.Allocator) !*Self {
        var engine = try allocator.create(Self);
        engine.allocator = allocator;
        engine.pager = try pager.Pager.initMemory(allocator);
        engine.memory_pool = memory_pool.MemoryPool.init(allocator);
        engine.pooled_allocator = memory_pool.PooledAllocator.init(&engine.memory_pool);
        engine.tables = std.StringHashMap(*Table).init(allocator);
        engine.indexes = std.StringHashMap(*Index).init(allocator);
        engine.is_memory = true;

        return engine;
    }

    /// Get the pooled allocator for efficient memory management
    pub fn getPooledAllocator(self: *Self) std.mem.Allocator {
        return self.pooled_allocator.allocator();
    }

    /// Get memory pool statistics
    pub fn getMemoryStats(self: *Self) memory_pool.MemoryPool.GlobalStats {
        return self.memory_pool.getGlobalStats();
    }

    /// Cleanup unused memory pools
    pub fn cleanupMemory(self: *Self) void {
        self.memory_pool.cleanup();
    }

    /// Create a new table
    pub fn createTable(self: *Self, name: []const u8, schema: TableSchema) !void {
        const table = try Table.create(self.allocator, self.pager, name, schema);
        try self.tables.put(try self.allocator.dupe(u8, name), table);

        // Persist table metadata if not in-memory
        if (!self.is_memory) {
            try self.saveTableMetadata(table);
        }
    }

    /// Get a table by name
    pub fn getTable(self: *Self, name: []const u8) ?*Table {
        return self.tables.get(name);
    }

    /// Get all table names in the database (v1.2.2 broad API)
    pub fn getTableNames(self: *Self, allocator: std.mem.Allocator) ![][]const u8 {
        var table_names = try allocator.alloc([]const u8, self.tables.count());
        var iterator = self.tables.iterator();
        var index: usize = 0;

        while (iterator.next()) |entry| {
            table_names[index] = try allocator.dupe(u8, entry.key_ptr.*);
            index += 1;
        }

        return table_names;
    }

    /// Drop a table
    pub fn dropTable(self: *Self, name: []const u8) !void {
        if (self.tables.fetchRemove(name)) |entry| {
            entry.value.deinit();
            self.allocator.free(entry.key);
        }
    }

    /// Create an index
    pub fn createIndex(self: *Self, name: []const u8, table_name: []const u8, column_names: [][]const u8, is_unique: bool) !void {
        const index = try Index.create(self.allocator, self.pager, name, table_name, column_names, is_unique);
        try self.indexes.put(try self.allocator.dupe(u8, name), index);
    }

    /// Get an index by name
    pub fn getIndex(self: *Self, name: []const u8) ?*Index {
        return self.indexes.get(name);
    }

    /// Drop an index
    pub fn dropIndex(self: *Self, name: []const u8) !void {
        if (self.indexes.fetchRemove(name)) |entry| {
            entry.value.deinit(self.allocator);
            self.allocator.free(entry.key);
        }
    }

    /// Load existing tables from storage
    fn loadTables(self: *Self) !void {
        // This would read table metadata from page 0 or a dedicated metadata area
        // For now, this is a placeholder
        _ = self;
    }

    /// Save table metadata to storage
    fn saveTableMetadata(self: *Self, table: *Table) !void {
        // This would write table schema to a metadata page
        _ = self;
        _ = table;
    }

    /// Clean up storage engine
    pub fn deinit(self: *Self) void {
        var table_iterator = self.tables.iterator();
        while (table_iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.tables.deinit();

        var index_iterator = self.indexes.iterator();
        while (index_iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.indexes.deinit();

        self.pager.deinit();
        self.memory_pool.deinit();
        self.allocator.destroy(self);
    }

    /// Get storage statistics
    pub fn getStats(self: *Self) StorageStats {
        const cache_stats = self.pager.getCacheStats();
        return StorageStats{
            .table_count = @intCast(self.tables.count()),
            .index_count = @intCast(self.indexes.count()),
            .is_memory = self.is_memory,
            .page_count = self.pager.getPageCount(),
            .cache_hit_ratio = cache_stats.hit_ratio,
            .cached_pages = cache_stats.cached_pages,
        };
    }
};

/// Table representation
pub const Table = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    schema: TableSchema,
    btree: *btree.BTree,
    row_count: u64,

    const Self = @This();

    /// Create a new table
    pub fn create(allocator: std.mem.Allocator, page_manager: *pager.Pager, name: []const u8, schema: TableSchema) !*Self {
        var table = try allocator.create(Self);
        table.allocator = allocator;
        table.name = try allocator.dupe(u8, name);
        // Deep clone schema to ensure ownership with storage engine's allocator
        table.schema = try schema.clone(allocator);
        table.btree = try btree.BTree.init(allocator, page_manager);
        table.row_count = 0;

        return table;
    }

    /// Insert a row into the table
    pub fn insert(self: *Self, row: Row) !void {
        try self.btree.insert(self.row_count, row);
        self.row_count += 1;
    }

    /// Select all rows
    pub fn select(self: *Self, allocator: std.mem.Allocator) ![]Row {
        return try self.btree.selectAll(allocator);
    }

    /// Clean up table
    pub fn deinit(self: *Self) void {
        self.btree.deinit();
        self.allocator.free(self.name);
        self.schema.deinit(self.allocator);
        self.allocator.destroy(self);
    }
};

/// Table schema definition
pub const TableSchema = struct {
    columns: []Column,

    pub fn deinit(self: *TableSchema, allocator: std.mem.Allocator) void {
        // Clean up column names and default values
        for (self.columns) |column| {
            allocator.free(column.name);
            if (column.default_value) |default_value| {
                default_value.deinit(allocator);
            }
        }
        allocator.free(self.columns);
    }

    /// Deep clone schema with a new allocator (for ownership transfer)
    pub fn clone(self: TableSchema, allocator: std.mem.Allocator) CloneValueError!TableSchema {
        var cloned_columns = try allocator.alloc(Column, self.columns.len);

        for (self.columns, 0..) |column, i| {
            cloned_columns[i] = Column{
                .name = try allocator.dupe(u8, column.name),
                .data_type = column.data_type,
                .is_primary_key = column.is_primary_key,
                .is_nullable = column.is_nullable,
                .default_value = if (column.default_value) |default_val|
                    try default_val.clone(allocator)
                else
                    null,
            };
        }

        return TableSchema{
            .columns = cloned_columns,
        };
    }
};

/// Column definition
pub const Column = struct {
    name: []const u8,
    data_type: DataType,
    is_primary_key: bool,
    is_nullable: bool,
    default_value: ?DefaultValue,

    pub const DefaultValue = union(enum) {
        Literal: Value,
        FunctionCall: FunctionCall,

        pub fn deinit(self: DefaultValue, allocator: std.mem.Allocator) void {
            switch (self) {
                .Literal => |value| value.deinit(allocator),
                .FunctionCall => |func| func.deinit(allocator),
            }
        }

        pub fn clone(self: DefaultValue, allocator: std.mem.Allocator) CloneValueError!DefaultValue {
            return switch (self) {
                .Literal => |value| DefaultValue{ .Literal = try value.clone(allocator) },
                .FunctionCall => |func| DefaultValue{ .FunctionCall = try func.clone(allocator) },
            };
        }
    };

    pub const FunctionCall = struct {
        name: []const u8,
        arguments: []FunctionArgument,

        pub fn deinit(self: FunctionCall, allocator: std.mem.Allocator) void {
            allocator.free(self.name);
            for (self.arguments) |arg| {
                arg.deinit(allocator);
            }
            allocator.free(self.arguments);
        }

        pub fn clone(self: FunctionCall, allocator: std.mem.Allocator) CloneValueError!FunctionCall {
            const cloned_name = try allocator.dupe(u8, self.name);
            var cloned_args = try allocator.alloc(FunctionArgument, self.arguments.len);

            for (self.arguments, 0..) |arg, i| {
                cloned_args[i] = try arg.clone(allocator);
            }

            return FunctionCall{
                .name = cloned_name,
                .arguments = cloned_args,
            };
        }
    };

    pub const FunctionArgument = union(enum) {
        Literal: Value,
        Column: []const u8,
        Parameter: u32,

        pub fn deinit(self: FunctionArgument, allocator: std.mem.Allocator) void {
            switch (self) {
                .Literal => |value| value.deinit(allocator),
                .Column => |col| allocator.free(col),
                .Parameter => {},
            }
        }

        pub fn clone(self: FunctionArgument, allocator: std.mem.Allocator) CloneValueError!FunctionArgument {
            return switch (self) {
                .Literal => |value| FunctionArgument{ .Literal = try value.clone(allocator) },
                .Column => |col| FunctionArgument{ .Column = try allocator.dupe(u8, col) },
                .Parameter => |param| FunctionArgument{ .Parameter = param },
            };
        }
    };
};

/// Supported data types
pub const DataType = enum {
    Integer,
    Text,
    Real,
    Blob,
    // PostgreSQL compatibility types
    JSON,
    JSONB,
    UUID,
    Array,
    Boolean,
    Timestamp,
    TimestampTZ,
    Date,
    Time,
    Interval,
    Numeric,
    // Extended integer types
    SmallInt,
    BigInt,
    // Extended text types
    Varchar,
    Char,
};

/// Row data
pub const Row = struct {
    values: []Value,

    pub fn deinit(self: *Row, allocator: std.mem.Allocator) void {
        for (self.values) |value| {
            value.deinit(allocator);
        }
        allocator.free(self.values);
    }
};

/// Value types
pub const Value = union(enum) {
    Integer: i64,
    Text: []const u8,
    Real: f64,
    Blob: []const u8,
    Null,
    Parameter: u32, // Parameter placeholder index
    FunctionCall: Column.FunctionCall, // Function call for evaluation (e.g., in INSERT VALUES)
    // PostgreSQL compatibility values
    JSON: []const u8, // JSON as text
    JSONB: JSONBValue, // Parsed JSON with binary optimization
    UUID: [16]u8, // UUID as 16 bytes
    Array: ArrayValue, // Array of values
    Boolean: bool,
    Timestamp: i64, // Unix timestamp in microseconds
    TimestampTZ: TimestampTZValue, // Timestamp with timezone
    Date: i32, // Days since epoch
    Time: i64, // Microseconds since midnight
    Interval: i64, // Duration in microseconds
    Numeric: NumericValue, // Arbitrary precision decimal
    SmallInt: i16,
    BigInt: i64,

    pub fn deinit(self: Value, allocator: std.mem.Allocator) void {
        switch (self) {
            .Text => |text| allocator.free(text),
            .Blob => |blob| allocator.free(blob),
            .FunctionCall => |func| func.deinit(allocator),
            .JSON => |json| allocator.free(json),
            .JSONB => |jsonb| jsonb.deinit(allocator),
            .Array => |array| array.deinit(allocator),
            .TimestampTZ => |tstz| tstz.deinit(allocator),
            .Numeric => |numeric| numeric.deinit(allocator),
            else => {},
        }
    }

    pub fn clone(self: Value, allocator: std.mem.Allocator) CloneValueError!Value {
        return switch (self) {
            .Integer => |i| Value{ .Integer = i },
            .Real => |r| Value{ .Real = r },
            .Null => Value.Null,
            .Parameter => |p| Value{ .Parameter = p },
            .Boolean => |b| Value{ .Boolean = b },
            .Timestamp => |t| Value{ .Timestamp = t },
            .Date => |d| Value{ .Date = d },
            .Time => |t| Value{ .Time = t },
            .Interval => |i| Value{ .Interval = i },
            .SmallInt => |s| Value{ .SmallInt = s },
            .BigInt => |b| Value{ .BigInt = b },
            .UUID => |u| Value{ .UUID = u },
            .Text => |text| Value{ .Text = try allocator.dupe(u8, text) },
            .Blob => |blob| Value{ .Blob = try allocator.dupe(u8, blob) },
            .JSON => |json| Value{ .JSON = try allocator.dupe(u8, json) },
            .FunctionCall => |func| Value{ .FunctionCall = try func.clone(allocator) },
            // For complex types, create proper deep copies
            .JSONB => |jsonb| blk: {
                const json_str = try jsonb.toString(allocator);
                defer allocator.free(json_str);
                break :blk Value{ .JSONB = try JSONBValue.init(allocator, json_str) };
            },
            .Array => |array| Value{ .Array = try array.clone(allocator) },
            .TimestampTZ => |tstz| Value{ .TimestampTZ = TimestampTZValue{
                .timestamp = tstz.timestamp,
                .timezone = try allocator.dupe(u8, tstz.timezone),
            } },
            .Numeric => |numeric| Value{ .Numeric = NumericValue{
                .precision = numeric.precision,
                .scale = numeric.scale,
                .digits = try allocator.dupe(u8, numeric.digits),
                .is_negative = numeric.is_negative,
            } },
        };
    }
};

/// JSONB value with parsed structure
pub const JSONBValue = struct {
    parsed: std.json.Parsed(std.json.Value),

    pub fn init(allocator: std.mem.Allocator, json_text: []const u8) !JSONBValue {
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_text, .{});
        return JSONBValue{ .parsed = parsed };
    }

    pub fn deinit(self: JSONBValue, allocator: std.mem.Allocator) void {
        _ = allocator;
        self.parsed.deinit();
    }

    pub fn toString(self: JSONBValue, allocator: std.mem.Allocator) ![]u8 {
        return try self.stringifyJson(allocator);
    }

    /// Convert parsed JSON back to string representation
    pub fn stringifyJson(self: JSONBValue, allocator: std.mem.Allocator) ![]u8 {
        return switch (self.parsed.value) {
            .null => try allocator.dupe(u8, "null"),
            .bool => |b| try allocator.dupe(u8, if (b) "true" else "false"),
            .integer => |i| try std.fmt.allocPrint(allocator, "{}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .number_string => |s| try allocator.dupe(u8, s),
            .string => |s| try std.fmt.allocPrint(allocator, "\"{s}\"", .{s}),
            .array => |arr| blk: {
                var result = std.ArrayList(u8){};
                defer result.deinit(allocator);

                try result.append(allocator, '[');
                for (arr.items, 0..) |item, i| {
                    if (i > 0) try result.appendSlice(allocator, ", ");
                    const item_json = JSONBValue{ .parsed = std.json.Parsed(std.json.Value){ .value = item, .arena = undefined } };
                    const item_str = try item_json.stringifyJson(allocator);
                    defer allocator.free(item_str);
                    try result.appendSlice(allocator, item_str);
                }
                try result.append(allocator, ']');
                break :blk try result.toOwnedSlice(allocator);
            },
            .object => |obj| blk: {
                var result = std.ArrayList(u8){};
                defer result.deinit(allocator);

                try result.append(allocator, '{');
                var first = true;
                var iterator = obj.iterator();
                while (iterator.next()) |entry| {
                    if (!first) try result.appendSlice(allocator, ", ");
                    first = false;
                    try result.appendSlice(allocator, "\"");
                    try result.appendSlice(allocator, entry.key_ptr.*);
                    try result.appendSlice(allocator, "\": ");

                    const value_json = JSONBValue{ .parsed = std.json.Parsed(std.json.Value){ .value = entry.value_ptr.*, .arena = undefined } };
                    const value_str = try value_json.stringifyJson(allocator);
                    defer allocator.free(value_str);
                    try result.appendSlice(allocator, value_str);
                }
                try result.append(allocator, '}');
                break :blk try result.toOwnedSlice(allocator);
            },
        };
    }

    /// Extract a value from JSON using a path (PostgreSQL -> operator)
    pub fn extractPath(self: JSONBValue, allocator: std.mem.Allocator, path: []const u8) !?[]u8 {
        const value = self.extractValue(path) orelse return null;
        const json_value = JSONBValue{ .parsed = std.json.Parsed(std.json.Value){ .value = value, .arena = undefined } };
        return try json_value.stringifyJson(allocator);
    }

    /// Extract a text value from JSON (PostgreSQL ->> operator)
    pub fn extractText(self: JSONBValue, allocator: std.mem.Allocator, path: []const u8) !?[]u8 {
        const value = self.extractValue(path) orelse return null;
        return switch (value) {
            .string => |s| try allocator.dupe(u8, s),
            .integer => |i| try std.fmt.allocPrint(allocator, "{}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .number_string => |s| try allocator.dupe(u8, s),
            .bool => |b| try allocator.dupe(u8, if (b) "true" else "false"),
            .null => try allocator.dupe(u8, "null"),
            else => {
                const json_value = JSONBValue{ .parsed = std.json.Parsed(std.json.Value){ .value = value, .arena = undefined } };
                return try json_value.stringifyJson(allocator);
            },
        };
    }

    /// Check if JSON contains a key (PostgreSQL ? operator)
    pub fn hasKey(self: JSONBValue, key: []const u8) bool {
        return switch (self.parsed.value) {
            .object => |obj| obj.contains(key),
            else => false,
        };
    }

    /// Extract raw JSON value for path operations
    fn extractValue(self: JSONBValue, path: []const u8) ?std.json.Value {
        return switch (self.parsed.value) {
            .object => |obj| obj.get(path),
            .array => |arr| blk: {
                const index = std.fmt.parseInt(usize, path, 10) catch return null;
                if (index >= arr.items.len) return null;
                break :blk arr.items[index];
            },
            else => null,
        };
    }
};

/// Array value containing typed elements
pub const ArrayValue = struct {
    element_type: DataType,
    elements: []Value,

    pub fn deinit(self: ArrayValue, allocator: std.mem.Allocator) void {
        for (self.elements) |element| {
            element.deinit(allocator);
        }
        allocator.free(self.elements);
    }

    /// Create array from values
    pub fn init(allocator: std.mem.Allocator, element_type: DataType, values: []const Value) CloneValueError!ArrayValue {
        var elements = try allocator.alloc(Value, values.len);

        // Clone each value
        for (values, 0..) |value, i| {
            elements[i] = try cloneValue(allocator, value);
        }

        return ArrayValue{
            .element_type = element_type,
            .elements = elements,
        };
    }

    /// Get array length
    pub fn len(self: ArrayValue) usize {
        return self.elements.len;
    }

    /// Get element at index (1-based like PostgreSQL)
    pub fn get(self: ArrayValue, index: usize) ?Value {
        if (index == 0 or index > self.elements.len) return null;
        return self.elements[index - 1];
    }

    /// Convert array to PostgreSQL format string: {elem1,elem2,elem3}
    pub fn toString(self: ArrayValue, allocator: std.mem.Allocator) ![]u8 {
        var result = std.ArrayList(u8){};
        defer result.deinit(allocator);

        try result.append(allocator, '{');

        for (self.elements, 0..) |element, i| {
            if (i > 0) try result.appendSlice(allocator, ",");

            const elem_str = try valueToString(allocator, element);
            defer allocator.free(elem_str);
            try result.appendSlice(allocator, elem_str);
        }

        try result.append(allocator, '}');
        return try result.toOwnedSlice(allocator);
    }

    /// Check if array contains value (PostgreSQL @> operator)
    pub fn contains(self: ArrayValue, value: Value) bool {
        for (self.elements) |element| {
            if (valuesEqual(element, value)) return true;
        }
        return false;
    }

    /// Array overlap (PostgreSQL && operator)
    pub fn overlaps(self: ArrayValue, other: ArrayValue) bool {
        for (self.elements) |element| {
            if (other.contains(element)) return true;
        }
        return false;
    }

    /// Clone the array
    pub fn clone(self: ArrayValue, allocator: std.mem.Allocator) CloneValueError!ArrayValue {
        return try ArrayValue.init(allocator, self.element_type, self.elements);
    }
};

/// Helper function to clone a value
const CloneValueError = error{
    OutOfMemory,
    Overflow,
    InvalidCharacter,
    UnexpectedToken,
    InvalidNumber,
    InvalidEnumTag,
    DuplicateField,
    UnknownField,
    MissingField,
    LengthMismatch,
    SyntaxError,
    UnexpectedEndOfInput,
    BufferUnderrun,
    ValueTooLong,
};

fn cloneValue(allocator: std.mem.Allocator, value: Value) CloneValueError!Value {
    return switch (value) {
        .Integer => |i| Value{ .Integer = i },
        .Real => |r| Value{ .Real = r },
        .Text => |t| Value{ .Text = try allocator.dupe(u8, t) },
        .Blob => |b| Value{ .Blob = try allocator.dupe(u8, b) },
        .Null => Value.Null,
        .JSON => |j| Value{ .JSON = try allocator.dupe(u8, j) },
        .JSONB => |jsonb| Value{ .JSONB = try JSONBValue.init(allocator, try jsonb.toString(allocator)) },
        .UUID => |uuid| Value{ .UUID = uuid },
        .Array => |array| Value{ .Array = try ArrayValue.init(allocator, array.element_type, array.elements) },
        .Boolean => |b| Value{ .Boolean = b },
        .SmallInt => |s| Value{ .SmallInt = s },
        .BigInt => |b| Value{ .BigInt = b },
        else => value, // For simple types that don't need cloning
    };
}

/// Helper function to convert value to string
fn valueToString(allocator: std.mem.Allocator, value: Value) ![]u8 {
    return switch (value) {
        .Integer => |i| try std.fmt.allocPrint(allocator, "{}", .{i}),
        .Real => |r| try std.fmt.allocPrint(allocator, "{d}", .{r}),
        .Text => |t| try std.fmt.allocPrint(allocator, "\"{s}\"", .{t}),
        .Boolean => |b| try allocator.dupe(u8, if (b) "true" else "false"),
        .Null => try allocator.dupe(u8, "NULL"),
        else => try allocator.dupe(u8, "?"), // Placeholder for complex types
    };
}

/// Helper function to compare values
fn valuesEqual(a: Value, b: Value) bool {
    return switch (a) {
        .Integer => |ai| switch (b) {
            .Integer => |bi| ai == bi,
            else => false,
        },
        .Real => |ar| switch (b) {
            .Real => |br| ar == br,
            else => false,
        },
        .Text => |at| switch (b) {
            .Text => |bt| std.mem.eql(u8, at, bt),
            else => false,
        },
        .Boolean => |ab| switch (b) {
            .Boolean => |bb| ab == bb,
            else => false,
        },
        .Null => switch (b) {
            .Null => true,
            else => false,
        },
        else => false, // Complex comparison would need more implementation
    };
}

/// Timestamp with timezone
pub const TimestampTZValue = struct {
    timestamp: i64, // Unix timestamp in microseconds
    timezone: []const u8, // Timezone name (e.g., "UTC", "America/New_York")

    pub fn deinit(self: TimestampTZValue, allocator: std.mem.Allocator) void {
        allocator.free(self.timezone);
    }
};

/// Arbitrary precision numeric value
pub const NumericValue = struct {
    precision: u16, // Total digits
    scale: u16, // Digits after decimal point
    digits: []u8, // BCD encoded digits
    is_negative: bool,

    pub fn deinit(self: NumericValue, allocator: std.mem.Allocator) void {
        allocator.free(self.digits);
    }
};

/// Storage statistics
pub const StorageStats = struct {
    table_count: u32,
    index_count: u32,
    is_memory: bool,
    page_count: u32,
    cache_hit_ratio: f64,
    cached_pages: u32,
};

/// Index definition
pub const Index = struct {
    name: []const u8,
    table_name: []const u8,
    column_names: [][]const u8,
    btree: *btree.BTree,
    is_unique: bool,

    const Self = @This();

    /// Create a new index
    pub fn create(allocator: std.mem.Allocator, page_manager: *pager.Pager, name: []const u8, table_name: []const u8, column_names: [][]const u8, is_unique: bool) !*Self {
        var index = try allocator.create(Self);
        index.name = try allocator.dupe(u8, name);
        index.table_name = try allocator.dupe(u8, table_name);

        // Clone column names
        index.column_names = try allocator.alloc([]const u8, column_names.len);
        for (column_names, 0..) |col_name, i| {
            index.column_names[i] = try allocator.dupe(u8, col_name);
        }

        index.btree = try btree.BTree.init(allocator, page_manager);
        index.is_unique = is_unique;

        return index;
    }

    /// Insert a key into the index
    pub fn insert(self: *Self, key: u64, row_id: u64) !void {
        if (self.is_unique) {
            // Check if key already exists
            if (try self.btree.search(key)) |_| {
                return error.UniqueConstraintViolation;
            }
        }

        // Create a row with just the row ID
        const index_row = Row{
            .values = try self.btree.allocator.alloc(Value, 1),
        };
        index_row.values[0] = Value{ .Integer = @intCast(row_id) };

        try self.btree.insert(key, index_row);
    }

    /// Search for a key in the index
    pub fn search(self: *Self, key: u64) !?u64 {
        if (try self.btree.search(key)) |row| {
            if (row.values.len > 0) {
                switch (row.values[0]) {
                    .Integer => |row_id| return @intCast(row_id),
                    else => return null,
                }
            }
        }
        return null;
    }

    /// Clean up index
    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.table_name);
        for (self.column_names) |col_name| {
            allocator.free(col_name);
        }
        allocator.free(self.column_names);
        self.btree.deinit();
        allocator.destroy(self);
    }
};

test "storage engine creation" {
    try std.testing.expect(true); // Placeholder
}

test "table operations" {
    try std.testing.expect(true); // Placeholder
}
