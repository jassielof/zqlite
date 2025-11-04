const std = @import("std");
const storage = @import("../db/storage.zig");
const ast = @import("../parser/ast.zig");
const Allocator = std.mem.Allocator;

pub const FunctionEvaluator = struct {
    const Self = @This();
    
    allocator: Allocator,
    
    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }
    
    pub fn evaluateFunction(self: *Self, function_call: ast.FunctionCall) !storage.Value {
        const func_name = function_call.name;
        
        // Convert function name to lowercase for case-insensitive comparison
        const lower_name = try std.ascii.allocLowerString(self.allocator, func_name);
        defer self.allocator.free(lower_name);
        
        if (std.mem.eql(u8, lower_name, "now")) {
            return self.evalNow(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "current_timestamp")) {
            return self.evalCurrentTimestamp(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "current_date")) {
            return self.evalCurrentDate(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "current_time")) {
            return self.evalCurrentTime(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "datetime")) {
            return self.evalDatetime(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "strftime")) {
            return self.evalStrftime(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "unixepoch")) {
            return self.evalUnixepoch(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "julianday")) {
            return self.evalJulianday(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "date")) {
            return self.evalDate(function_call.arguments);
        } else if (std.mem.eql(u8, lower_name, "time")) {
            return self.evalTime(function_call.arguments);
        } else {
            return error.UnknownFunction;
        }
    }
    
    fn evalNow(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len != 0) {
            return error.InvalidArgumentCount;
        }

        // Return current timestamp as ISO 8601 string
        const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const timestamp = ts.sec;
        const datetime_str = try self.formatTimestamp(timestamp);
        return storage.Value{ .Text = datetime_str };
    }
    
    fn evalDatetime(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len == 0) {
            return self.evalNow(&[_]ast.FunctionArgument{});
        }
        
        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }
        
        const arg = arguments[0];
        switch (arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| {
                        if (std.mem.eql(u8, text, "now")) {
                            return self.evalNow(&[_]ast.FunctionArgument{});
                        } else {
                            // Parse and format the datetime string
                            return storage.Value{ .Text = try self.allocator.dupe(u8, text) };
                        }
                    },
                    .Integer => |timestamp| {
                        const datetime_str = try self.formatTimestamp(timestamp);
                        return storage.Value{ .Text = datetime_str };
                    },
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
    }
    
    fn evalStrftime(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len != 2) {
            return error.InvalidArgumentCount;
        }
        
        const format_arg = arguments[0];
        const time_arg = arguments[1];
        
        var format_str: []const u8 = undefined;
        var timestamp: i64 = undefined;
        
        // Get format string
        switch (format_arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| format_str = text,
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
        
        // Get timestamp
        switch (time_arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| {
                        if (std.mem.eql(u8, text, "now")) {
                            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                            timestamp = ts.sec;
                        } else {
                            // Try to parse as datetime string
                            timestamp = try self.parseTimestamp(text);
                        }
                    },
                    .Integer => |ts| timestamp = ts,
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
        
        // Format the timestamp
        const formatted = try self.formatTimestampWithFormat(timestamp, format_str);
        return storage.Value{ .Text = formatted };
    }
    
    fn evalUnixepoch(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len == 0) {
            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            return storage.Value{ .Integer = ts.sec };
        }

        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const arg = arguments[0];
        switch (arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| {
                        if (std.mem.eql(u8, text, "now")) {
                            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                            return storage.Value{ .Integer = ts.sec };
                        } else {
                            const timestamp = try self.parseTimestamp(text);
                            return storage.Value{ .Integer = timestamp };
                        }
                    },
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
    }
    
    fn evalJulianday(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len == 0) {
            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            const timestamp = ts.sec;
            const julian_day = self.timestampToJulianDay(timestamp);
            return storage.Value{ .Real = julian_day };
        }

        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const arg = arguments[0];
        switch (arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| {
                        var timestamp: i64 = undefined;
                        if (std.mem.eql(u8, text, "now")) {
                            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                            timestamp = ts.sec;
                        } else {
                            timestamp = try self.parseTimestamp(text);
                        }
                        const julian_day = self.timestampToJulianDay(timestamp);
                        return storage.Value{ .Real = julian_day };
                    },
                    .Integer => |timestamp| {
                        const julian_day = self.timestampToJulianDay(timestamp);
                        return storage.Value{ .Real = julian_day };
                    },
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
    }
    
    fn evalDate(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len == 0) {
            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            const timestamp = ts.sec;
            const date_str = try self.formatDate(timestamp);
            return storage.Value{ .Text = date_str };
        }

        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const arg = arguments[0];
        switch (arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| {
                        var timestamp: i64 = undefined;
                        if (std.mem.eql(u8, text, "now")) {
                            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                            timestamp = ts.sec;
                        } else {
                            timestamp = try self.parseTimestamp(text);
                        }
                        const date_str = try self.formatDate(timestamp);
                        return storage.Value{ .Text = date_str };
                    },
                    .Integer => |timestamp| {
                        const date_str = try self.formatDate(timestamp);
                        return storage.Value{ .Text = date_str };
                    },
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
    }
    
    fn evalTime(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len == 0) {
            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            const timestamp = ts.sec;
            const time_str = try self.formatTime(timestamp);
            return storage.Value{ .Text = time_str };
        }

        if (arguments.len != 1) {
            return error.InvalidArgumentCount;
        }

        const arg = arguments[0];
        switch (arg) {
            .Literal => |value| {
                switch (value) {
                    .Text => |text| {
                        var timestamp: i64 = undefined;
                        if (std.mem.eql(u8, text, "now")) {
                            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
                            timestamp = ts.sec;
                        } else {
                            timestamp = try self.parseTimestamp(text);
                        }
                        const time_str = try self.formatTime(timestamp);
                        return storage.Value{ .Text = time_str };
                    },
                    .Integer => |timestamp| {
                        const time_str = try self.formatTime(timestamp);
                        return storage.Value{ .Text = time_str };
                    },
                    else => return error.InvalidArgumentType,
                }
            },
            else => return error.InvalidArgumentType,
        }
    }

    fn evalCurrentTimestamp(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len != 0) {
            return error.InvalidArgumentCount;
        }

        // Return current timestamp as ISO 8601 string
        const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const timestamp = ts.sec;
        const datetime_str = try self.formatTimestamp(timestamp);
        return storage.Value{ .Text = datetime_str };
    }

    fn evalCurrentDate(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len != 0) {
            return error.InvalidArgumentCount;
        }

        // Return current date as YYYY-MM-DD string
        const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const timestamp = ts.sec;
        const date_str = try self.formatDate(timestamp);
        return storage.Value{ .Text = date_str };
    }

    fn evalCurrentTime(self: *Self, arguments: []ast.FunctionArgument) !storage.Value {
        if (arguments.len != 0) {
            return error.InvalidArgumentCount;
        }

        // Return current time as HH:MM:SS string
        const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const timestamp = ts.sec;
        const time_str = try self.formatTime(timestamp);
        return storage.Value{ .Text = time_str };
    }

    fn formatTimestamp(self: *Self, timestamp: i64) ![]u8 {
        // Format as ISO 8601: YYYY-MM-DD HH:MM:SS
        // Use a more accurate approach for current timestamps
        const epoch_seconds = @as(u64, @intCast(@max(timestamp, 0)));

        // Convert to time components (simplified for now - use proper calendar math in production)
        const days_since_epoch = epoch_seconds / 86400;
        const seconds_in_day = epoch_seconds % 86400;

        // Approximate year (more accurate would be complex)
        const year = 1970 + (days_since_epoch / 365);
        // Simplified month/day
        const day_in_year = days_since_epoch % 365;
        const month = 1 + (day_in_year / 30);
        const day = 1 + (day_in_year % 30);

        const hour = seconds_in_day / 3600;
        const minute = (seconds_in_day % 3600) / 60;
        const second = seconds_in_day % 60;

        return std.fmt.allocPrint(self.allocator, "{d:4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
            year, month, day, hour, minute, second
        });
    }
    
    fn formatTimestampWithFormat(self: *Self, timestamp: i64, format: []const u8) ![]u8 {
        // Simple format implementation - in production, use proper strftime
        if (std.mem.eql(u8, format, "%s")) {
            return std.fmt.allocPrint(self.allocator, "{d}", .{timestamp});
        } else if (std.mem.eql(u8, format, "%Y-%m-%d %H:%M:%S")) {
            return self.formatTimestamp(timestamp);
        } else if (std.mem.eql(u8, format, "%Y-%m-%d")) {
            return self.formatDate(timestamp);
        } else if (std.mem.eql(u8, format, "%H:%M:%S")) {
            return self.formatTime(timestamp);
        } else {
            // Default to ISO format
            return self.formatTimestamp(timestamp);
        }
    }
    
    fn formatDate(self: *Self, timestamp: i64) ![]u8 {
        const epoch_seconds = @as(u64, @intCast(@max(timestamp, 0)));
        const days_since_epoch = epoch_seconds / 86400;

        // Approximate year (simplified)
        const year = 1970 + (days_since_epoch / 365);
        const day_in_year = days_since_epoch % 365;
        const month = 1 + (day_in_year / 30);
        const day = 1 + (day_in_year % 30);

        return std.fmt.allocPrint(self.allocator, "{d:4}-{d:0>2}-{d:0>2}", .{
            year, month, day
        });
    }
    
    fn formatTime(self: *Self, timestamp: i64) ![]u8 {
        const epoch_seconds = @as(u64, @intCast(timestamp));
        const seconds_in_day = epoch_seconds % 86400;
        
        const hour = seconds_in_day / 3600;
        const minute = (seconds_in_day % 3600) / 60;
        const second = seconds_in_day % 60;
        
        return std.fmt.allocPrint(self.allocator, "{d:0>2}:{d:0>2}:{d:0>2}", .{
            hour, minute, second
        });
    }
    
    fn parseTimestamp(self: *Self, datetime_str: []const u8) !i64 {
        _ = self;
        // Simplified parser - in production, use proper datetime parsing
        if (std.mem.eql(u8, datetime_str, "now")) {
            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            return ts.sec;
        }

        // Try to parse as Unix timestamp
        if (std.fmt.parseInt(i64, datetime_str, 10)) |timestamp| {
            return timestamp;
        } else |_| {
            // For now, return current timestamp for unparseable strings
            const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
            return ts.sec;
        }
    }
    
    fn timestampToJulianDay(self: *Self, timestamp: i64) f64 {
        _ = self;
        // Convert Unix timestamp to Julian Day Number
        // Unix epoch (1970-01-01) is JD 2440587.5
        const unix_epoch_jd = 2440587.5;
        const seconds_per_day = 86400.0;
        return unix_epoch_jd + (@as(f64, @floatFromInt(timestamp)) / seconds_per_day);
    }
};

test "datetime function evaluation" {
    const allocator = std.testing.allocator;
    
    var evaluator = FunctionEvaluator.init(allocator);
    
    // Test NOW() function
    const now_args = [_]ast.FunctionArgument{};
    const now_result = try evaluator.evaluateFunction(ast.FunctionCall{
        .name = "now",
        .arguments = @constCast(&now_args),
    });
    defer now_result.deinit(allocator);
    
    try std.testing.expect(now_result == .Text);
    
    // Test DATETIME('now') function
    const datetime_args = [_]ast.FunctionArgument{
        ast.FunctionArgument{ .Literal = ast.Value{ .Text = "now" } },
    };
    const datetime_result = try evaluator.evaluateFunction(ast.FunctionCall{
        .name = "datetime",
        .arguments = @constCast(&datetime_args),
    });
    defer datetime_result.deinit(allocator);
    
    try std.testing.expect(datetime_result == .Text);
    
    // Test UNIXEPOCH() function
    const unixepoch_args = [_]ast.FunctionArgument{};
    const unixepoch_result = try evaluator.evaluateFunction(ast.FunctionCall{
        .name = "unixepoch",
        .arguments = @constCast(&unixepoch_args),
    });
    
    try std.testing.expect(unixepoch_result == .Integer);
    
    // Test STRFTIME('%s', 'now') function
    const strftime_args = [_]ast.FunctionArgument{
        ast.FunctionArgument{ .Literal = ast.Value{ .Text = "%s" } },
        ast.FunctionArgument{ .Literal = ast.Value{ .Text = "now" } },
    };
    const strftime_result = try evaluator.evaluateFunction(ast.FunctionCall{
        .name = "strftime",
        .arguments = @constCast(&strftime_args),
    });
    defer strftime_result.deinit(allocator);
    
    try std.testing.expect(strftime_result == .Text);
}