const std = @import("std");
const tokioz = @import("tokioz");

// Export post-quantum QUIC transport
pub const PQQuicTransport = @import("pq_quic.zig").PQQuicTransport;
pub const PQDatabaseTransport = @import("pq_quic.zig").PQDatabaseTransport;

/// 🌐 ZQLite Transport Layer
/// High-performance networking with optional post-quantum features
pub const Transport = struct {
    allocator: std.mem.Allocator,
    endpoint: ?Endpoint,
    connections: std.array_list.Managed(*Connection),
    is_server: bool,
    crypto_enabled: bool,

    const Self = @This();
    const ConnectionId = u64;

    const Endpoint = struct {
        address: std.net.Address,
        socket: std.net.Stream,
    };

    const Connection = struct {
        id: ConnectionId,
        endpoint: std.net.Address,
        state: ConnectionState,
        write_buffer: std.array_list.Managed(u8),
        read_buffer: std.array_list.Managed(u8),

        const ConnectionState = enum {
            Connecting,
            Connected,
            Closing,
            Closed,
        };

        pub fn init(allocator: std.mem.Allocator, id: ConnectionId, endpoint: std.net.Address) !*Connection {
            const conn = try allocator.create(Connection);
            conn.* = Connection{
                .id = id,
                .endpoint = endpoint,
                .state = .Connecting,
                .write_buffer = std.array_list.Managed(u8).init(allocator),
                .read_buffer = std.array_list.Managed(u8).init(allocator),
            };
            return conn;
        }

        pub fn deinit(self: *Connection, allocator: std.mem.Allocator) void {
            self.write_buffer.deinit();
            self.read_buffer.deinit();
            allocator.destroy(self);
        }
    };

    pub fn init(allocator: std.mem.Allocator, is_server: bool) Self {
        return Self{
            .allocator = allocator,
            .endpoint = null,
            .connections = std.array_list.Managed(*Connection).init(allocator),
            .is_server = is_server,
            .crypto_enabled = false,
        };
    }

    pub fn bind(self: *Self, address: std.net.Address) !void {
        const socket = try std.net.tcpConnectToAddress(address);
        self.endpoint = Endpoint{
            .address = address,
            .socket = socket,
        };
    }

    pub fn connect(self: *Self, server_address: std.net.Address) !ConnectionId {
        const ts = std.posix.clock_gettime(std.posix.CLOCK.REALTIME) catch unreachable;
        const timestamp = ts.sec;
        const conn_id = @as(ConnectionId, @intCast(timestamp));
        const connection = try Connection.init(self.allocator, conn_id, server_address);
        connection.state = .Connected;
        try self.connections.append(connection);
        return conn_id;
    }

    pub fn sendData(self: *Self, conn_id: ConnectionId, data: []const u8) !void {
        for (self.connections.items) |conn| {
            if (conn.id == conn_id) {
                try conn.write_buffer.appendSlice(data);
                return;
            }
        }
        return error.ConnectionNotFound;
    }

    pub fn receiveData(self: *Self, conn_id: ConnectionId) ![]u8 {
        for (self.connections.items) |conn| {
            if (conn.id == conn_id) {
                const data = try self.allocator.dupe(u8, conn.read_buffer.items);
                conn.read_buffer.clearRetainingCapacity();
                return data;
            }
        }
        return error.ConnectionNotFound;
    }

    pub fn closeConnection(self: *Self, conn_id: ConnectionId) !void {
        for (self.connections.items, 0..) |conn, i| {
            if (conn.id == conn_id) {
                conn.state = .Closed;
                conn.deinit(self.allocator);
                _ = self.connections.orderedRemove(i);
                return;
            }
        }
    }

    pub fn getStats(self: Self) TransportStats {
        return TransportStats{
            .active_connections = self.connections.items.len,
            .total_bytes_sent = 0,
            .total_bytes_received = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.connections.items) |conn| {
            conn.deinit(self.allocator);
        }
        self.connections.deinit();

        if (self.endpoint) |endpoint| {
            endpoint.socket.close();
        }
    }
};

pub const TransportStats = struct {
    active_connections: usize,
    total_bytes_sent: u64,
    total_bytes_received: u64,
};

// Test for transport functionality
test "transport basic functionality" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var transport = Transport.init(allocator, false);
    defer transport.deinit();

    const server_addr = std.net.Address.parseIp("127.0.0.1", 8080) catch unreachable;
    const conn_id = try transport.connect(server_addr);

    try testing.expect(conn_id != 0);
    try testing.expect(transport.getStats().active_connections == 1);

    try transport.closeConnection(conn_id);
    try testing.expect(transport.getStats().active_connections == 0);
}
