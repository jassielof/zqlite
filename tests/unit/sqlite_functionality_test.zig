const std = @import("std");
const testing = std.testing;
const zqlite = @import("zqlite");

test "SQLite Basic CRUD Operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create in-memory database
    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Test CREATE TABLE
    try conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)");

    // Test INSERT
    try conn.execute("INSERT INTO users (name, age) VALUES ('Alice', 25)");
    try conn.execute("INSERT INTO users (name, age) VALUES ('Bob', 30)");
    try conn.execute("INSERT INTO users (name, age) VALUES ('Charlie', 35)");

    // Test SELECT COUNT
    var stmt = try conn.prepare("SELECT COUNT(*) FROM users");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 1);
    const count_value = result.rows.items[0].values[0];
    switch (count_value) {
        .Integer => |count| try testing.expect(count == 3),
        else => return error.UnexpectedValueType,
    }
}

test "SQLite Data Types Support" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Test various SQLite data types
    try conn.execute(
        \\CREATE TABLE type_test (
        \\  id INTEGER PRIMARY KEY,
        \\  text_col TEXT,
        \\  real_col REAL,
        \\  blob_col BLOB,
        \\  null_col TEXT
        \\)
    );

    try conn.execute("INSERT INTO type_test (text_col, real_col, null_col) VALUES ('hello', 3.14, NULL)");

    var stmt = try conn.prepare("SELECT text_col, real_col, null_col FROM type_test WHERE id = 1");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 1);
    const row = result.rows.items[0];

    // Test TEXT type
    switch (row.values[0]) {
        .Text => |text| try testing.expectEqualStrings("hello", text),
        else => return error.UnexpectedValueType,
    }

    // Test REAL type
    switch (row.values[1]) {
        .Real => |real| try testing.expect(@abs(real - 3.14) < 0.01),
        else => return error.UnexpectedValueType,
    }

    // Test NULL type
    switch (row.values[2]) {
        .Null => {},
        else => return error.ExpectedNull,
    }
}

test "SQLite WHERE Clauses and Filtering" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Setup test data
    try conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)");
    try conn.execute("INSERT INTO products (name, price, category) VALUES ('Laptop', 999.99, 'Electronics')");
    try conn.execute("INSERT INTO products (name, price, category) VALUES ('Book', 19.99, 'Education')");
    try conn.execute("INSERT INTO products (name, price, category) VALUES ('Phone', 599.99, 'Electronics')");
    try conn.execute("INSERT INTO products (name, price, category) VALUES ('Pen', 2.99, 'Office')");

    // Test numeric comparison
    var stmt = try conn.prepare("SELECT name FROM products WHERE price > 500");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 2); // Laptop and Phone

    // Test text comparison
    var stmt2 = try conn.prepare("SELECT name FROM products WHERE category = 'Electronics'");
    defer stmt2.deinit();

    var result2 = try stmt2.execute(conn);
    defer result2.deinit(allocator);

    try testing.expect(result2.rows.items.len == 2); // Laptop and Phone
}

test "SQLite UPDATE and DELETE Operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER)");
    try conn.execute("INSERT INTO inventory (item, quantity) VALUES ('Apples', 100)");
    try conn.execute("INSERT INTO inventory (item, quantity) VALUES ('Bananas', 50)");
    try conn.execute("INSERT INTO inventory (item, quantity) VALUES ('Oranges', 75)");

    // Test UPDATE
    try conn.execute("UPDATE inventory SET quantity = 120 WHERE item = 'Apples'");

    var stmt = try conn.prepare("SELECT quantity FROM inventory WHERE item = 'Apples'");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 1);
    switch (result.rows.items[0].values[0]) {
        .Integer => |qty| try testing.expect(qty == 120),
        else => return error.UnexpectedValueType,
    }

    // Test DELETE
    try conn.execute("DELETE FROM inventory WHERE item = 'Bananas'");

    var stmt2 = try conn.prepare("SELECT COUNT(*) FROM inventory");
    defer stmt2.deinit();

    var result2 = try stmt2.execute(conn);
    defer result2.deinit(allocator);

    switch (result2.rows.items[0].values[0]) {
        .Integer => |count| try testing.expect(count == 2), // Should have 2 items left
        else => return error.UnexpectedValueType,
    }
}

test "SQLite JOINS" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Setup tables for JOIN test
    try conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
    try conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product TEXT, amount REAL)");

    try conn.execute("INSERT INTO customers (name) VALUES ('Alice')");
    try conn.execute("INSERT INTO customers (name) VALUES ('Bob')");

    try conn.execute("INSERT INTO orders (customer_id, product, amount) VALUES (1, 'Laptop', 999.99)");
    try conn.execute("INSERT INTO orders (customer_id, product, amount) VALUES (1, 'Mouse', 29.99)");
    try conn.execute("INSERT INTO orders (customer_id, product, amount) VALUES (2, 'Keyboard', 79.99)");

    // Test INNER JOIN
    var stmt = try conn.prepare(
        \\SELECT c.name, o.product, o.amount
        \\FROM customers c
        \\INNER JOIN orders o ON c.id = o.customer_id
        \\WHERE c.name = 'Alice'
    );
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 2); // Alice has 2 orders
}

test "SQLite GROUP BY and Aggregation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    try conn.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL)");
    try conn.execute("INSERT INTO sales (region, amount) VALUES ('North', 1000.00)");
    try conn.execute("INSERT INTO sales (region, amount) VALUES ('North', 1500.00)");
    try conn.execute("INSERT INTO sales (region, amount) VALUES ('South', 800.00)");
    try conn.execute("INSERT INTO sales (region, amount) VALUES ('South', 1200.00)");

    // Test GROUP BY with SUM
    var stmt = try conn.prepare("SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 2);

    // Check North region total
    switch (result.rows.items[0].values[1]) {
        .Real => |total| try testing.expect(@abs(total - 2500.0) < 0.01),
        else => return error.UnexpectedValueType,
    }
}

test "SQLite DEFAULT CURRENT_TIMESTAMP" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try zqlite.open(allocator, ":memory:");
    defer conn.close();

    // Test CREATE TABLE with DEFAULT CURRENT_TIMESTAMP
    try conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)");

    // Insert without specifying timestamp
    try conn.execute("INSERT INTO events (name) VALUES ('Test Event')");

    var stmt = try conn.prepare("SELECT name, created_at FROM events WHERE id = 1");
    defer stmt.deinit();

    var result = try stmt.execute(conn);
    defer result.deinit(allocator);

    try testing.expect(result.rows.items.len == 1);

    // Verify that created_at is not null and contains a timestamp
    switch (result.rows.items[0].values[1]) {
        .Text => |timestamp| {
            try testing.expect(timestamp.len > 0);
            // Should contain date format like "2025-10-08 13:34:14"
            try testing.expect(std.mem.indexOf(u8, timestamp, "-") != null);
        },
        else => return error.ExpectedTimestamp,
    }
}
