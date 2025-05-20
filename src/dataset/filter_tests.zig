const std = @import("std");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectError = testing.expectError;

const types = @import("types.zig");
const TypeId = types.TypeId;
const DatasetError = types.DatasetError;
const Dataset = @import("core.zig").Dataset;
const filter = @import("filter.zig");
const ComparisonOp = filter.ComparisonOp;

test "Dataset - basic filtering with integers" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const ages = [_]i32{ 25, 30, 22, 40, 35 };
    const scores = [_]f32{ 85.5, 90.0, 77.5, 95.5, 88.0 };
    
    try ds.addColumn("id", &ids);
    try ds.addColumn("age", &ages);
    try ds.addColumn("score", &scores);

    // Filter rows where age > 30
    var filtered = try filter.filter(&ds, "age", .GreaterThan, 30);
    defer filtered.deinit();

    // Verify filtered dataset
    try expectEqual(@as(usize, 2), filtered.row_count);
    
    const filtered_ids = try filtered.getColumn("id", i32);
    try expectEqual(@as(i32, 4), filtered_ids[0]);
    try expectEqual(@as(i32, 5), filtered_ids[1]);
    
    const filtered_ages = try filtered.getColumn("age", i32);
    try expectEqual(@as(i32, 40), filtered_ages[0]);
    try expectEqual(@as(i32, 35), filtered_ages[1]);
}

test "Dataset - filtering with floats" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const scores = [_]f32{ 85.5, 90.0, 77.5, 95.5, 88.0 };
    
    try ds.addColumn("id", &ids);
    try ds.addColumn("score", &scores);

    // Filter rows where score >= 90
    var filtered = try filter.filter(&ds, "score", .GreaterThanOrEqual, 90.0);
    defer filtered.deinit();

    // Verify filtered dataset
    try expectEqual(@as(usize, 2), filtered.row_count);
    
    const filtered_ids = try filtered.getColumn("id", i32);
    try expectEqual(@as(i32, 2), filtered_ids[0]);
    try expectEqual(@as(i32, 4), filtered_ids[1]);
    
    const filtered_scores = try filtered.getColumn("score", f32);
    try expectEqual(@as(f32, 90.0), filtered_scores[0]);
    try expectEqual(@as(f32, 95.5), filtered_scores[1]);
}

test "Dataset - filtering with strings" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const names = [_][]const u8{ "Alice", "Bob", "Charlie", "David", "Eve" };
    
    try ds.addColumn("id", &ids);
    try ds.addColumn("name", &names);

    // Filter rows where name starts with "A"
    var filtered = try filter.filter(&ds, "name", .StartsWith, "A");
    defer filtered.deinit();

    // Verify filtered dataset
    try expectEqual(@as(usize, 1), filtered.row_count);
    
    const filtered_ids = try filtered.getColumn("id", i32);
    try expectEqual(@as(i32, 1), filtered_ids[0]);
    
    const filtered_names = try filtered.getColumn("name", []const u8);
    try expect(std.mem.eql(u8, "Alice", filtered_names[0]));
}

test "Dataset - filtering with custom predicate" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const ages = [_]i32{ 25, 30, 22, 40, 35 };
    const names = [_][]const u8{ "Alice", "Bob", "Charlie", "David", "Eve" };
    
    try ds.addColumn("id", &ids);
    try ds.addColumn("age", &ages);
    try ds.addColumn("name", &names);

    // Define a custom predicate: age > 25 and name starts with a vowel
    const predicate = struct {
        fn apply(row_idx: usize, dataset: *const Dataset) bool {
            const age = dataset.getColumnValue(row_idx, "age", i32) catch return false;
            const name = dataset.getColumnValue(row_idx, "name", []const u8) catch return false;
            
            if (age <= 25) return false;
            
            const first_char = name[0];
            return first_char == 'A' or first_char == 'E' or 
                   first_char == 'I' or first_char == 'O' or 
                   first_char == 'U';
        }
    }.apply;

    // Filter using the custom predicate
    var filtered = try filter.filterWithPredicate(&ds, predicate);
    defer filtered.deinit();

    // Verify filtered dataset
    try expectEqual(@as(usize, 1), filtered.row_count);
    
    const filtered_ids = try filtered.getColumn("id", i32);
    try expectEqual(@as(i32, 5), filtered_ids[0]); // Only Eve (age 35) matches
    
    const filtered_names = try filtered.getColumn("name", []const u8);
    try expect(std.mem.eql(u8, "Eve", filtered_names[0]));
}

test "Dataset - filtering with nullable values" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data with nullable values
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const nullable_ages = [_]?i32{ 25, null, 22, 40, null };
    
    try ds.addColumn("id", &ids);
    try ds.addColumn("age", &nullable_ages);

    // Filter rows where age is not null
    var filtered = try filter.filter(&ds, "age", .IsNotNull, {});
    defer filtered.deinit();

    // Verify filtered dataset
    try expectEqual(@as(usize, 3), filtered.row_count);
    
    const filtered_ids = try filtered.getColumn("id", i32);
    try expectEqual(@as(i32, 1), filtered_ids[0]);
    try expectEqual(@as(i32, 3), filtered_ids[1]);
    try expectEqual(@as(i32, 4), filtered_ids[2]);
    
    const filtered_ages = try filtered.getColumn("age", ?i32);
    try expectEqual(@as(?i32, 25), filtered_ages[0]);
    try expectEqual(@as(?i32, 22), filtered_ages[1]);
    try expectEqual(@as(?i32, 40), filtered_ages[2]);
}

test "Dataset - combining filters with logical operations" {
    const allocator = testing.allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    // Add test data
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const ages = [_]i32{ 25, 30, 22, 40, 35 };
    const scores = [_]f32{ 85.5, 90.0, 77.5, 95.5, 88.0 };
    
    try ds.addColumn("id", &ids);
    try ds.addColumn("age", &ages);
    try ds.addColumn("score", &scores);

    // Filter rows where age > 25
    var filtered1 = try filter.filter(&ds, "age", .GreaterThan, 25);
    defer filtered1.deinit();

    // Filter rows where score >= 90
    var filtered2 = try filter.filter(&ds, "score", .GreaterThanOrEqual, 90.0);
    defer filtered2.deinit();

    // Combine filters with logical AND (intersection)
    var combined = try filter.logicalAnd(&filtered1, &filtered2);
    defer combined.deinit();

    // Verify combined dataset (should only contain row with id=4, age=40, score=95.5)
    try expectEqual(@as(usize, 1), combined.row_count);
    
    const combined_ids = try combined.getColumn("id", i32);
    try expectEqual(@as(i32, 4), combined_ids[0]);
    
    const combined_ages = try combined.getColumn("age", i32);
    try expectEqual(@as(i32, 40), combined_ages[0]);
    
    const combined_scores = try combined.getColumn("score", f32);
    try expectEqual(@as(f32, 95.5), combined_scores[0]);
}

// Helper function for Dataset to get a value at a specific row and column
// This is needed for the custom predicate test
fn getColumnValue(self: *const Dataset, row_idx: usize, column_name: []const u8, comptime T: type) !T {
    if (row_idx >= self.rows) return error.InvalidIndex;
    
    const col_idx = self.getColumnIndex(column_name) orelse return error.ColumnNotFound;
    const column = self.columns.items[col_idx];
    
    switch (column.type_id) {
        .int => {
            if (T != i32) return error.TypeMismatch;
            const data = @as([*]i32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
            return data[row_idx];
        },
        .float => {
            if (T != f32) return error.TypeMismatch;
            const data = @as([*]f32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
            return data[row_idx];
        },
        .string => {
            if (T != []const u8) return error.TypeMismatch;
            const data = @as([*][]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
            return data[row_idx];
        },
        .boolean => {
            if (T != bool) return error.TypeMismatch;
            const data = @as([*]bool, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
            return data[row_idx];
        },
        else => return error.UnsupportedType,
    }
}
