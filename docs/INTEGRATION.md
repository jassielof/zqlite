# 🔗 ZQLite Integration Guide

> **ZQLite v1.2.1** - Zig-native embedded database and query engine

This guide shows how to integrate ZQLite into your Zig projects for general database needs.

---

## 📋 **Quick Start**

### Installation

```bash
# Clone the repository
git clone https://github.com/ghostkellz/zqlite.git
cd zqlite

# Build the library
zig build

# Run tests
zig test src/zqlite.zig
```

### Basic Integration

Add to your `build.zig`:

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    
    // Add zqlite dependency
    const zqlite = b.dependency("zqlite", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "your-app",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Import zqlite module
    exe.root_module.addImport("zqlite", zqlite.module("zqlite"));

    b.installArtifact(exe);
}
```

---

## 🚀 **Core API Usage**

### 1. **Database Connection**

```zig
const std = @import("std");
const zqlite = @import("zqlite");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open file database
    var conn = try zqlite.open("myapp.db");
    defer conn.close();
    
    // OR open in-memory database
    var mem_conn = try zqlite.openMemory();
    defer mem_conn.close();
}
```

### 2. **Creating Tables**

```zig
// Basic table creation
try conn.execute(
    \\CREATE TABLE users (
    \\    id INTEGER,
    \\    name TEXT,
    \\    email TEXT,
    \\    created_at TEXT DEFAULT (datetime('now'))
    \\)
);

// Table with constraints
try conn.execute(
    \\CREATE TABLE products (
    \\    id INTEGER PRIMARY KEY,
    \\    name TEXT NOT NULL,
    \\    price REAL NOT NULL,
    \\    category_id INTEGER,
    \\    FOREIGN KEY (category_id) REFERENCES categories(id)
    \\)
);
```

### 3. **Inserting Data**

```zig
// Simple insert
try conn.execute("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')");

// Multiple inserts
try conn.execute("INSERT INTO users (id, name, email) VALUES (2, 'Jane', 'jane@example.com')");
try conn.execute("INSERT INTO users (id, name, email) VALUES (3, 'Bob', 'bob@example.com')");
```

### 4. **Prepared Statements**

```zig
// Create prepared statement
var stmt = try conn.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
defer stmt.deinit();

// Bind parameters and execute
try stmt.bind(0, 4);                    // Integer
try stmt.bind(1, "Alice");              // String  
try stmt.bind(2, "alice@example.com");  // String
try stmt.execute(conn);

// Reset and reuse
stmt.reset();
try stmt.bind(0, 5);
try stmt.bind(1, "Charlie");
try stmt.bind(2, "charlie@example.com");
try stmt.execute(conn);
```

### 5. **Querying Data** 

```zig
// Basic SELECT - results are printed to stdout by default
try conn.execute("SELECT * FROM users");
try conn.execute("SELECT name, email FROM users WHERE id > 2");

// Using prepared statements for queries
var query_stmt = try conn.prepare("SELECT * FROM users WHERE id = ?");
defer query_stmt.deinit();

try query_stmt.bind(0, 1);
try query_stmt.execute(conn);
```

### 6. **Transactions**

```zig
// Manual transaction control
try conn.begin();
defer conn.rollback() catch {}; // Rollback on error

try conn.execute("INSERT INTO users (id, name) VALUES (10, 'User10')");
try conn.execute("INSERT INTO users (id, name) VALUES (11, 'User11')");

try conn.commit(); // Only commits if no errors

// Batch transaction helper
const batch_sql = [_][]const u8{
    "INSERT INTO users (id, name) VALUES (20, 'User20')",
    "INSERT INTO users (id, name) VALUES (21, 'User21')",
    "INSERT INTO users (id, name) VALUES (22, 'User22')",
};

try conn.transactionExec(&batch_sql);
```

### 7. **Working with Different Data Types**

```zig
// Create table with various types
try conn.execute(
    \\CREATE TABLE data_types (
    \\    id INTEGER,
    \\    name TEXT,
    \\    price REAL,
    \\    is_active INTEGER,  -- Boolean as INTEGER (0/1)
    \\    data BLOB,
    \\    created_date TEXT DEFAULT (date('now'))
    \\)
);

// Insert different data types
var stmt = try conn.prepare("INSERT INTO data_types (id, name, price, is_active) VALUES (?, ?, ?, ?)");
defer stmt.deinit();

try stmt.bind(0, 1);           // INTEGER
try stmt.bind(1, "Product");   // TEXT
try stmt.bind(2, 29.99);       // REAL
try stmt.bind(1, true);        // BOOLEAN (auto-converted to 1)
try stmt.execute(conn);
```

---

## 🏗️ **Project Structure Examples**

### Simple Application

```zig
// src/database.zig
const std = @import("std");
const zqlite = @import("zqlite");

pub const AppDatabase = struct {
    conn: *zqlite.Connection,
    allocator: std.mem.Allocator,
    
    pub fn init(allocator: std.mem.Allocator, db_path: []const u8) !AppDatabase {
        const conn = try zqlite.open(db_path);
        
        var db = AppDatabase{
            .conn = conn,
            .allocator = allocator,
        };
        
        try db.initSchema();
        return db;
    }
    
    pub fn deinit(self: *AppDatabase) void {
        self.conn.close();
    }
    
    fn initSchema(self: *AppDatabase) !void {
        try self.conn.execute(
            \\CREATE TABLE IF NOT EXISTS users (
            \\    id INTEGER PRIMARY KEY,
            \\    username TEXT UNIQUE NOT NULL,
            \\    email TEXT UNIQUE NOT NULL,
            \\    created_at TEXT DEFAULT (datetime('now'))
            \\)
        );
        
        try self.conn.execute(
            \\CREATE TABLE IF NOT EXISTS posts (
            \\    id INTEGER PRIMARY KEY,
            \\    user_id INTEGER NOT NULL,
            \\    title TEXT NOT NULL,
            \\    content TEXT NOT NULL,
            \\    created_at TEXT DEFAULT (datetime('now')),
            \\    FOREIGN KEY (user_id) REFERENCES users(id)
            \\)
        );
    }
    
    // Application-specific methods
    pub fn createUser(self: *AppDatabase, username: []const u8, email: []const u8) !void {
        var stmt = try self.conn.prepare("INSERT INTO users (username, email) VALUES (?, ?)");
        defer stmt.deinit();
        
        try stmt.bind(0, username);
        try stmt.bind(1, email);
        try stmt.execute(self.conn);
    }
    
    pub fn getUserPosts(self: *AppDatabase, user_id: i64) !void {
        var stmt = try self.conn.prepare(
            \\SELECT p.title, p.content, p.created_at 
            \\FROM posts p 
            \\WHERE p.user_id = ?
            \\ORDER BY p.created_at DESC
        );
        defer stmt.deinit();
        
        try stmt.bind(0, user_id);
        try stmt.execute(self.conn);
    }
};
```

```zig
// src/main.zig
const std = @import("std");
const AppDatabase = @import("database.zig").AppDatabase;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var db = try AppDatabase.init(allocator, "app.db");
    defer db.deinit();
    
    // Create some users
    try db.createUser("alice", "alice@example.com");
    try db.createUser("bob", "bob@example.com");
    
    // Query user posts
    try db.getUserPosts(1);
    
    std.log.info("Application completed successfully");
}
```

### Web API Backend

```zig
// src/api.zig  
const std = @import("std");
const zqlite = @import("zqlite");

pub const ApiServer = struct {
    db: *zqlite.Connection,
    allocator: std.mem.Allocator,
    
    pub fn init(allocator: std.mem.Allocator) !ApiServer {
        const db = try zqlite.open("api.db");
        
        var server = ApiServer{
            .db = db,
            .allocator = allocator,
        };
        
        try server.setupTables();
        return server;
    }
    
    pub fn deinit(self: *ApiServer) void {
        self.db.close();
    }
    
    fn setupTables(self: *ApiServer) !void {
        // API keys table
        try self.db.execute(
            \\CREATE TABLE IF NOT EXISTS api_keys (
            \\    id INTEGER PRIMARY KEY,
            \\    key_hash TEXT UNIQUE NOT NULL,
            \\    user_id INTEGER NOT NULL,
            \\    created_at TEXT DEFAULT (datetime('now')),
            \\    last_used TEXT,
            \\    is_active INTEGER DEFAULT 1
            \\)
        );
        
        // Request logs table
        try self.db.execute(
            \\CREATE TABLE IF NOT EXISTS request_logs (
            \\    id INTEGER PRIMARY KEY,
            \\    api_key_id INTEGER,
            \\    endpoint TEXT NOT NULL,
            \\    method TEXT NOT NULL,
            \\    response_code INTEGER,
            \\    timestamp TEXT DEFAULT (datetime('now')),
            \\    FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
            \\)
        );
        
        // Create indexes for performance
        try self.db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)");
        try self.db.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs(timestamp)");
    }
    
    pub fn authenticateRequest(self: *ApiServer, api_key: []const u8) !bool {
        var stmt = try self.db.prepare(
            \\SELECT id FROM api_keys 
            \\WHERE key_hash = ? AND is_active = 1
        );
        defer stmt.deinit();
        
        try stmt.bind(0, api_key);
        try stmt.execute(self.db);
        
        // Update last_used timestamp
        var update_stmt = try self.db.prepare("UPDATE api_keys SET last_used = datetime('now') WHERE key_hash = ?");
        defer update_stmt.deinit();
        
        try update_stmt.bind(0, api_key);
        try update_stmt.execute(self.db);
        
        return true; // Simplified - would check actual query results
    }
    
    pub fn logRequest(self: *ApiServer, api_key: []const u8, endpoint: []const u8, method: []const u8, response_code: i32) !void {
        var stmt = try self.db.prepare(
            \\INSERT INTO request_logs (api_key_id, endpoint, method, response_code) 
            \\VALUES ((SELECT id FROM api_keys WHERE key_hash = ?), ?, ?, ?)
        );
        defer stmt.deinit();
        
        try stmt.bind(0, api_key);
        try stmt.bind(1, endpoint);
        try stmt.bind(2, method);
        try stmt.bind(3, response_code);
        try stmt.execute(self.db);
    }
    
    pub fn getApiStats(self: *ApiServer) !void {
        try self.db.execute(
            \\SELECT 
            \\    COUNT(*) as total_requests,
            \\    COUNT(DISTINCT api_key_id) as active_keys,
            \\    AVG(response_code) as avg_response_code
            \\FROM request_logs 
            \\WHERE date(timestamp) = date('now')
        );
    }
};
```

---

## 📊 **Advanced Features**

### Date/Time Functions

```zig
// ZQLite supports SQLite-compatible datetime functions
try conn.execute(
    \\CREATE TABLE events (
    \\    id INTEGER PRIMARY KEY,
    \\    name TEXT,
    \\    created_at TEXT DEFAULT (datetime('now')),
    \\    event_date TEXT DEFAULT (date('now')),
    \\    event_time TEXT DEFAULT (time('now'))
    \\)
);

// Query with date functions
try conn.execute("SELECT * FROM events WHERE date(created_at) = date('now')");
try conn.execute("SELECT name, datetime(created_at) FROM events WHERE created_at > datetime('now', '-1 day')");
```

### JSON-like Data (using TEXT)

```zig
// Store JSON-like data as TEXT
try conn.execute(
    \\CREATE TABLE configs (
    \\    id INTEGER PRIMARY KEY,
    \\    app_name TEXT,
    \\    settings TEXT  -- JSON as TEXT
    \\)
);

// Insert JSON data
try conn.execute("INSERT INTO configs (app_name, settings) VALUES ('myapp', '{\"theme\": \"dark\", \"notifications\": true}')");
```

### Indexes for Performance

```zig
// Create indexes on frequently queried columns
try conn.execute("CREATE INDEX idx_users_email ON users(email)");
try conn.execute("CREATE INDEX idx_posts_user_id ON posts(user_id)");
try conn.execute("CREATE INDEX idx_posts_created_at ON posts(created_at)");

// Composite indexes
try conn.execute("CREATE INDEX idx_posts_user_date ON posts(user_id, created_at)");
```

---

## ⚡ **Performance Tips**

### 1. **Use Transactions for Bulk Operations**

```zig
// ✅ Good: Batch inserts in transaction
try conn.begin();
defer conn.rollback() catch {};

for (0..1000) |i| {
    var stmt = try conn.prepare("INSERT INTO data (value) VALUES (?)");
    defer stmt.deinit();
    try stmt.bind(0, @as(i64, @intCast(i)));
    try stmt.execute(conn);
}

try conn.commit();

// ✅ Even better: Use batch helper
const batch_inserts = [_][]const u8{
    "INSERT INTO data (value) VALUES (1)",
    "INSERT INTO data (value) VALUES (2)", 
    // ... up to 1000
};
try conn.transactionExec(&batch_inserts);
```

### 2. **Reuse Prepared Statements**

```zig
// ✅ Good: Prepare once, execute many
var stmt = try conn.prepare("INSERT INTO users (name, email) VALUES (?, ?)");
defer stmt.deinit();

const users = [_]struct { name: []const u8, email: []const u8 }{
    .{ .name = "Alice", .email = "alice@example.com" },
    .{ .name = "Bob", .email = "bob@example.com" },
    .{ .name = "Charlie", .email = "charlie@example.com" },
};

for (users) |user| {
    try stmt.bind(0, user.name);
    try stmt.bind(1, user.email);
    try stmt.execute(conn);
    stmt.reset(); // Reset for next execution
}
```

### 3. **Create Appropriate Indexes**

```zig
// Create indexes for WHERE clauses
try conn.execute("CREATE INDEX idx_users_active ON users(is_active) WHERE is_active = 1");

// Create indexes for JOIN columns  
try conn.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)");

// Create indexes for ORDER BY columns
try conn.execute("CREATE INDEX idx_posts_created_at ON posts(created_at DESC)");
```

---

## 🔧 **Error Handling**

```zig
const std = @import("std");
const zqlite = @import("zqlite");

pub fn handleDatabaseOperations() !void {
    var conn = zqlite.open("test.db") catch |err| switch (err) {
        error.FileNotFound => {
            std.log.warn("Database file not found, creating new one");
            return zqlite.open("test.db");
        },
        error.AccessDenied => {
            std.log.err("Permission denied accessing database");
            return err;
        },
        else => return err,
    };
    defer conn.close();
    
    // Handle SQL execution errors
    conn.execute("CREATE TABLE test (id INTEGER)") catch |err| switch (err) {
        error.TableExists => {
            std.log.info("Table already exists, continuing...");
        },
        error.SyntaxError => {
            std.log.err("SQL syntax error in CREATE TABLE");
            return err;
        },
        else => return err,
    };
    
    // Handle constraint violations
    conn.execute("INSERT INTO test (id) VALUES (1)") catch |err| switch (err) {
        error.ConstraintViolation => {
            std.log.warn("Constraint violation, record may already exist");
        },
        else => return err,
    };
}
```

---

## 🧪 **Testing with ZQLite**

```zig
// tests/database_test.zig
const std = @import("std");
const testing = std.testing;
const zqlite = @import("zqlite");

test "database basic operations" {
    // Use in-memory database for tests
    var conn = try zqlite.openMemory();
    defer conn.close();
    
    // Create test table
    try conn.execute("CREATE TABLE test_users (id INTEGER, name TEXT)");
    
    // Insert test data
    try conn.execute("INSERT INTO test_users (id, name) VALUES (1, 'Test User')");
    
    // Verify data exists (would need query result parsing for full verification)
    try conn.execute("SELECT COUNT(*) FROM test_users");
    
    // Test prepared statements
    var stmt = try conn.prepare("INSERT INTO test_users (id, name) VALUES (?, ?)");
    defer stmt.deinit();
    
    try stmt.bind(0, 2);
    try stmt.bind(1, "Second User");
    try stmt.execute(conn);
}

test "transaction rollback" {
    var conn = try zqlite.openMemory();
    defer conn.close();
    
    try conn.execute("CREATE TABLE test_tx (id INTEGER)");
    
    // Test transaction rollback
    try conn.begin();
    try conn.execute("INSERT INTO test_tx (id) VALUES (1)");
    try conn.rollback();
    
    // Verify rollback worked (record should not exist)
    try conn.execute("SELECT COUNT(*) FROM test_tx"); // Should be 0
}
```

---

## 📦 **Complete Example: Task Manager**

```zig
// Complete example: Simple task manager
const std = @import("std");
const zqlite = @import("zqlite");

const TaskStatus = enum {
    pending,
    in_progress, 
    completed,
    
    pub fn toString(self: TaskStatus) []const u8 {
        return switch (self) {
            .pending => "pending",
            .in_progress => "in_progress",
            .completed => "completed",
        };
    }
};

const TaskManager = struct {
    db: *zqlite.Connection,
    allocator: std.mem.Allocator,
    
    pub fn init(allocator: std.mem.Allocator, db_path: []const u8) !TaskManager {
        const db = try zqlite.open(db_path);
        
        var manager = TaskManager{
            .db = db,
            .allocator = allocator,
        };
        
        try manager.initSchema();
        return manager;
    }
    
    pub fn deinit(self: *TaskManager) void {
        self.db.close();
    }
    
    fn initSchema(self: *TaskManager) !void {
        try self.db.execute(
            \\CREATE TABLE IF NOT EXISTS tasks (
            \\    id INTEGER PRIMARY KEY,
            \\    title TEXT NOT NULL,
            \\    description TEXT,
            \\    status TEXT DEFAULT 'pending',
            \\    priority INTEGER DEFAULT 3,
            \\    created_at TEXT DEFAULT (datetime('now')),
            \\    updated_at TEXT DEFAULT (datetime('now')),
            \\    due_date TEXT
            \\)
        );
        
        // Create index for common queries
        try self.db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)");
        try self.db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_due_date ON tasks(due_date)");
    }
    
    pub fn addTask(self: *TaskManager, title: []const u8, description: []const u8, priority: i32) !void {
        var stmt = try self.db.prepare("INSERT INTO tasks (title, description, priority) VALUES (?, ?, ?)");
        defer stmt.deinit();
        
        try stmt.bind(0, title);
        try stmt.bind(1, description);
        try stmt.bind(2, priority);
        try stmt.execute(self.db);
        
        std.log.info("Task added: {s}", .{title});
    }
    
    pub fn updateTaskStatus(self: *TaskManager, task_id: i64, status: TaskStatus) !void {
        var stmt = try self.db.prepare("UPDATE tasks SET status = ?, updated_at = datetime('now') WHERE id = ?");
        defer stmt.deinit();
        
        try stmt.bind(0, status.toString());
        try stmt.bind(1, task_id);
        try stmt.execute(self.db);
        
        std.log.info("Task {} status updated to: {s}", .{ task_id, status.toString() });
    }
    
    pub fn listTasks(self: *TaskManager, status: ?TaskStatus) !void {
        const query = if (status) |s|
            "SELECT id, title, status, priority, created_at FROM tasks WHERE status = ? ORDER BY priority ASC, created_at DESC"
        else
            "SELECT id, title, status, priority, created_at FROM tasks ORDER BY priority ASC, created_at DESC";
            
        if (status) |s| {
            var stmt = try self.db.prepare(query);
            defer stmt.deinit();
            try stmt.bind(0, s.toString());
            try stmt.execute(self.db);
        } else {
            try self.db.execute(query);
        }
    }
    
    pub fn getTaskStats(self: *TaskManager) !void {
        try self.db.execute(
            \\SELECT 
            \\    status,
            \\    COUNT(*) as count,
            \\    AVG(priority) as avg_priority
            \\FROM tasks 
            \\GROUP BY status 
            \\ORDER BY status
        );
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var task_manager = try TaskManager.init(allocator, "tasks.db");
    defer task_manager.deinit();
    
    // Add some tasks
    try task_manager.addTask("Implement user authentication", "Add login/logout functionality", 1);
    try task_manager.addTask("Write documentation", "Create API documentation", 2);
    try task_manager.addTask("Fix bug #123", "Resolve memory leak in parser", 1);
    
    // Update task status
    try task_manager.updateTaskStatus(1, .in_progress);
    try task_manager.updateTaskStatus(3, .completed);
    
    // List tasks
    std.log.info("All tasks:");
    try task_manager.listTasks(null);
    
    std.log.info("\nPending tasks:");
    try task_manager.listTasks(.pending);
    
    std.log.info("\nTask statistics:");
    try task_manager.getTaskStats();
    
    std.log.info("Task manager demo completed!");
}
```

---

## 🔗 **Related Documentation**

- **[CRYPTO_PROJECTS.md](CRYPTO_PROJECTS.md)** - Advanced crypto/blockchain integration examples
- **[CLAUDE.md](CLAUDE.md)** - Development roadmap and refactoring notes  
- **[examples/](examples/)** - Working code examples and demos

---

## 🆘 **Troubleshooting**

### Common Issues

1. **Segmentation fault on cleanup**
   - **Fixed in v1.2.1** - Memory management improvements
   - Use `defer connection.close()` to ensure cleanup

2. **SQL parsing errors**
   - ZQLite supports core SQLite syntax
   - Some advanced features like aggregate functions (SUM, COUNT) are still in development
   - Stick to basic CREATE, INSERT, SELECT, UPDATE, DELETE operations

3. **Build errors**
   - Ensure you're using a compatible Zig version
   - Check that zsync dependency is available
   - Run `zig build` from the project root

### Performance Issues

1. **Slow bulk inserts**
   - Use transactions for batch operations
   - Use prepared statements for repeated operations  
   - Consider using `transactionExec()` for simple batch operations

2. **Slow queries**
   - Create indexes on frequently queried columns
   - Use prepared statements instead of dynamic SQL
   - Check your WHERE clauses use indexed columns

---

*ZQLite v1.2.1 - A lightweight, embeddable database for Zig applications*