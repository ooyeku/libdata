const std = @import("std");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectError = testing.expectError;

const types = @import("types.zig");
const TypeId = types.TypeId;
const DatasetError = types.DatasetError;
const Dataset = @import("core.zig").Dataset;

test "Dataset - basic initialization" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    try expectEqual(@as(usize, 0), ds.rows);
    try expectEqual(@as(usize, 0), ds.row_count);
    try expectEqual(@as(usize, 0), ds.column_count);
}

test "Dataset - add and get columns" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Test integer column
    const int_data = [_]i32{ 1, 2, 3, 4, 5 };
    try ds.addColumn("integers", &int_data);
    const retrieved_ints = try ds.getColumn("integers", i32);
    try expectEqual(@as(usize, 5), retrieved_ints.len);
    try expectEqual(@as(i32, 1), retrieved_ints[0]);
    try expectEqual(@as(i32, 5), retrieved_ints[4]);

    // Test float column
    const float_data = [_]f32{ 1.1, 2.2, 3.3, 4.4, 5.5 };
    try ds.addColumn("floats", &float_data);
    const retrieved_floats = try ds.getColumn("floats", f32);
    try expectEqual(@as(usize, 5), retrieved_floats.len);
    try expectEqual(@as(f32, 1.1), retrieved_floats[0]);
    try expectEqual(@as(f32, 5.5), retrieved_floats[4]);

    // Test string column
    const string_data = [_][]const u8{ "a", "b", "c", "d", "e" };
    try ds.addColumn("strings", &string_data);
    const retrieved_strings = try ds.getColumn("strings", []const u8);
    try expectEqual(@as(usize, 5), retrieved_strings.len);
    try expect(std.mem.eql(u8, "a", retrieved_strings[0]));
    try expect(std.mem.eql(u8, "e", retrieved_strings[4]));
}

test "Dataset - error cases" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Test column not found
    try expectError(DatasetError.ColumnNotFound, ds.getColumn("nonexistent", i32));

    // Test length mismatch
    const data1 = [_]i32{ 1, 2, 3 };
    try ds.addColumn("col1", &data1);

    const data2 = [_]i32{ 1, 2 }; // Different length
    try expectError(DatasetError.LengthMismatch, ds.addColumn("col2", &data2));
}

test "Dataset - CSV operations" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Create test data
    const ids = [_]i32{ 1, 2, 3 };
    const values = [_]f32{ 1.1, 2.2, 3.3 };
    const names = [_][]const u8{ "a", "b", "c" };

    try ds.addColumn("id", &ids);
    try ds.addColumn("value", &values);
    try ds.addColumn("name", &names);

    // Write to CSV
    try ds.toCSV("test.csv");

    // Read from CSV
    var ds2 = Dataset.init(allocator);
    defer ds2.deinit();
    try ds2.fromCSV("test.csv");

    // Verify data
    try expectEqual(ds.row_count, ds2.row_count);
    try expectEqual(ds.column_count, ds2.column_count);

    // Clean up test file
    try std.fs.cwd().deleteFile("test.csv");
}

test "Dataset - nullable values" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const nullable_ints = [_]?i32{ 1, null, 3 };
    try ds.addColumn("nullable_ints", &nullable_ints);

    const retrieved = try ds.getColumn("nullable_ints", ?i32);
    try expectEqual(@as(?i32, 1), retrieved[0]);
    try expectEqual(@as(?i32, null), retrieved[1]);
    try expectEqual(@as(?i32, 3), retrieved[2]);
}

test "Dataset - column operations" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Test hasColumn
    try expect(!ds.hasColumn("test"));
    const data = [_]i32{1};
    try ds.addColumn("test", &data);
    try expect(ds.hasColumn("test"));

    // Test columnType
    const type_id = ds.columnType("test");
    try expect(type_id != null);
    try expectEqual(TypeId.int, type_id.?);
    try expect(ds.columnType("nonexistent") == null);
}

test "Dataset - formatting" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const data = [_]i32{ 1, 2, 3 };
    try ds.addColumn("numbers", &data);

    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();
    try ds.format("", .{}, list.writer());

    // Verify output contains expected strings
    const output = list.items;
    try expect(std.mem.indexOf(u8, output, "Dataset with") != null);
    try expect(std.mem.indexOf(u8, output, "numbers") != null);
}

test "Dataset - column statistics" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const data = [_]i32{ 1, 5, 3, 9, 2 };
    try ds.addColumn("values", &data);

    const stats = @import("stats.zig");
    const min = try stats.columnMin(&ds, "values");
    const max = try stats.columnMax(&ds, "values");

    try expectEqual(@as(f32, 1), min);
    try expectEqual(@as(f32, 9), max);
}

test "Dataset - len function" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const data = [_]i32{ 1, 2, 3, 4, 5 };
    try ds.addColumn("values", &data);

    try expectEqual(@as(usize, 5), ds.len());
}

test "Dataset - column names" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const data1 = [_]i32{ 1, 2, 3 };
    const data2 = [_]f32{ 1.1, 2.2, 3.3 };
    try ds.addColumn("col1", &data1);
    try ds.addColumn("col2", &data2);

    // Verify we can access column names
    try expect(ds.hasColumn("col1"));
    try expect(ds.hasColumn("col2"));
    try expect(!ds.hasColumn("col3"));
    
    try expectEqual(@as(usize, 2), ds.column_count);
}

test "Dataset - column access" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const data = [_]i32{ 1, 2, 3 };
    try ds.addColumn("values", &data);

    // Test column existence
    try expect(ds.hasColumn("values"));
    try expect(!ds.hasColumn("nonexistent"));
    
    // Test column type
    try expect(ds.columnType("values") != null);
    try expectEqual(TypeId.int, ds.columnType("values").?);
}

test "Dataset - copy column subset" {
    // Use an arena allocator to simplify cleanup
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit(); // This will free all allocations at once
    const allocator = arena.allocator();
    
    var source_ds = Dataset.init(allocator);
    defer source_ds.deinit();

    var target_ds = Dataset.init(allocator);
    defer target_ds.deinit();

    const data = [_]i32{ 1, 2, 3, 4, 5 };
    try source_ds.addColumn("values", &data);

    // Create a column in target with different data
    const target_data = [_]i32{ 10, 20, 30, 40, 50 };
    try target_ds.addColumn("target_values", &target_data);

    // Copy column from source to target (full range)
    try source_ds.copyColumnTo(&target_ds, "values", 0, source_ds.row_count);

    // Verify the copied column
    const copied = try target_ds.getColumn("values", i32);
    try expectEqual(@as(usize, 5), copied.len);
    try expectEqual(@as(i32, 1), copied[0]);
    try expectEqual(@as(i32, 5), copied[4]);
    
    // Create another dataset for subset testing
    var subset_ds = Dataset.init(allocator);
    defer subset_ds.deinit();
    
    // Copy a subset
    try source_ds.copyColumnTo(&subset_ds, "values", 1, 4);
    
    // Verify the subset
    const subset = try subset_ds.getColumn("values", i32);
    try expectEqual(@as(usize, 3), subset.len);
    try expectEqual(@as(i32, 2), subset[0]);
    try expectEqual(@as(i32, 4), subset[2]);
}

test "Dataset - column stats" {
    // Use an arena allocator to simplify cleanup
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data
    const data = [_]i32{ 1, 5, 3, 9, 2 };
    try ds.addColumn("values", &data);

    // Test basic stats functions
    const stats = @import("stats.zig");
    
    // Min/Max
    const min = try stats.columnMin(&ds, "values");
    const max = try stats.columnMax(&ds, "values");
    try expectEqual(@as(f32, 1), min);
    try expectEqual(@as(f32, 9), max);
    
    // Range
    const range = try stats.columnRange(&ds, "values");
    try expectEqual(@as(f32, 8), range);
    
    // Mean
    const mean = try stats.columnMean(&ds, "values");
    try expectEqual(@as(f32, 4), mean);
    
    // Variance
    const variance = try stats.columnVariance(&ds, "values");
    try expect(variance > 0);
    
    // Standard Deviation
    const std_dev = try stats.columnStdDev(&ds, "values");
    try expect(std_dev > 0);
    
    // Median
    const median = try stats.columnMedian(&ds, "values");
    try expectEqual(@as(f32, 3), median);
    
    // Percentile
    const p25 = try stats.columnPercentile(&ds, "values", 25);
    const p50 = try stats.columnPercentile(&ds, "values", 50);
    const p75 = try stats.columnPercentile(&ds, "values", 75);
    try expect(p25 <= p50);
    try expect(p50 <= p75);
    
    // Q1, Q3, IQR
    const q1 = try stats.columnQ1(&ds, "values");
    const q3 = try stats.columnQ3(&ds, "values");
    const iqr = try stats.columnIQR(&ds, "values");
    try expectEqual(q3 - q1, iqr);
}

test "Dataset - display large dataset" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Create a dataset with many rows
    const row_count = 1000000;
    var ids = try allocator.alloc(i32, row_count);
    defer allocator.free(ids);

    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        ids[i] = @intCast(i);
    }

    try ds.addColumn("id", ids);

    // Format the dataset
    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();

    try ds.format("", .{}, list.writer());

    // Verify output contains expected strings
    const output = list.items;
    try expect(std.mem.indexOf(u8, output, "Dataset with 1000000 rows") != null);
    
    // The default format might truncate the output for large datasets
    // so we should see some indication of this
    try expect(std.mem.indexOf(u8, output, "id") != null);
}
