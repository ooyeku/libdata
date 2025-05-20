const std = @import("std");
const core = @import("core.zig");
const types = @import("types.zig");
const utils = @import("utils.zig");
const TypeId = types.TypeId;
const Dataset = core.Dataset;
const DatasetError = types.DatasetError;

/// Comparison operators for filtering
pub const ComparisonOp = enum {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
};

/// Predicate function type for custom filtering
pub const PredicateFn = fn(row_idx: usize, dataset: *const Dataset) bool;

/// Filter dataset based on a condition
pub fn filter(dataset: *const Dataset, column_name: []const u8, op: ComparisonOp, value: anytype) !Dataset {
    // For IsNull and IsNotNull operations, we don't need a value parameter
    if (op == .IsNull or op == .IsNotNull) {
        // Just call the filter function directly with the column and operation
        const col_idx = dataset.getColumnIndex(column_name) orelse return error.ColumnNotFound;
        const column = dataset.columns.items[col_idx];
        
        // Create a mask for the rows that match the condition
        const mask = try dataset.allocator.alloc(bool, dataset.rows);
        defer dataset.allocator.free(mask);
        
        // Check if the column type is nullable
        switch (column.type_id) {
            .nullable_int, .nullable_float, .nullable_string, .nullable_boolean, .nullable_char => {
                // For nullable types, apply the IsNull/IsNotNull check directly
                // We'll handle each nullable type separately
                switch (column.type_id) {
                    .nullable_int => {
                        const data = @as([*]?i32, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
                        for (data, 0..) |item, i| {
                            mask[i] = if (op == .IsNull) (item == null) else (item != null);
                        }
                    },
                    .nullable_float => {
                        const data = @as([*]?f32, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
                        for (data, 0..) |item, i| {
                            mask[i] = if (op == .IsNull) (item == null) else (item != null);
                        }
                    },
                    .nullable_string => {
                        const data = @as([*]?[]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
                        for (data, 0..) |item, i| {
                            mask[i] = if (op == .IsNull) (item == null) else (item != null);
                        }
                    },
                    .nullable_boolean => {
                        const data = @as([*]?bool, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
                        for (data, 0..) |item, i| {
                            mask[i] = if (op == .IsNull) (item == null) else (item != null);
                        }
                    },
                    .nullable_char => {
                        const data = @as([*]?u8, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
                        for (data, 0..) |item, i| {
                            mask[i] = if (op == .IsNull) (item == null) else (item != null);
                        }
                    },
                    else => {}, // Should not happen as we already checked for nullable types
                }
            },
            else => {
                // For non-nullable types, IsNull is always false, IsNotNull is always true
                for (0..dataset.rows) |i| {
                    mask[i] = (op == .IsNotNull);
                }
            },
        }
        
        // Create a filtered dataset based on the mask
        var result = Dataset.init(dataset.allocator);
        errdefer result.deinit();
        
        // Count how many rows match the filter
        var matched_rows: usize = 0;
        for (mask) |m| {
            if (m) matched_rows += 1;
        }
        
        // Copy column definitions
        for (dataset.columns.items) |col| {
            try result.addColumn(col.name, col.type_id);
        }
        
        // Copy data for each column
        if (matched_rows > 0) {
            for (dataset.columns.items, 0..) |_, i| {
                try utils.copyColumnDataFiltered(dataset, &result, i, mask, matched_rows);
            }
        }
        
        return result;
    }
    const T = @TypeOf(value);
    var result = Dataset.init(dataset.allocator);
    errdefer result.deinit();

    // First check if column exists
    const col_idx = dataset.getColumnIndex(column_name) orelse return error.ColumnNotFound;
    const column = dataset.columns.items[col_idx];

    // Validate type compatibility
    switch (column.type_id) {
        .int => {
            if (T != i32 and T != u32 and T != i64 and T != u64 and T != comptime_int) {
                return error.TypeMismatch;
            }
        },
        .nullable_int => {
            if (T != ?i32 and T != i32 and T != u32 and T != i64 and T != u64 and T != comptime_int) {
                return error.TypeMismatch;
            }
        },
        .float => {
            if (T != f32 and T != f64 and T != comptime_float) {
                return error.TypeMismatch;
            }
        },
        .nullable_float => {
            if (T != ?f32 and T != f32 and T != f64 and T != comptime_float) {
                return error.TypeMismatch;
            }
        },
        .string => {
            // Allow any type for string comparison - we'll handle conversion in the filter logic
            // This makes it more flexible for users to filter with string literals
        },
        .nullable_string => {
            // Allow any type for string comparison - we'll handle conversion in the filter logic
            // This makes it more flexible for users to filter with string literals
        },
        .boolean => {
            if (T != bool) {
                return error.TypeMismatch;
            }
        },
        .nullable_boolean => {
            if (T != ?bool and T != bool) {
                return error.TypeMismatch;
            }
        },
        else => return error.UnsupportedType,
    }

    // Create a mask of rows that match the condition
    var mask = try dataset.allocator.alloc(bool, dataset.rows);
    defer dataset.allocator.free(mask);

    for (mask) |*m| {
        m.* = false;
    }

    // Apply the filter based on the column type
    switch (column.type_id) {
        .int => {
            const data = @as([*]i32, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                // Handle different numeric types for comparison
                if (@TypeOf(value) == comptime_int) {
                    const int_value: i32 = @intCast(value);
                    mask[i] = evaluateIntCondition(item, op, int_value);
                } else if (@TypeOf(value) == comptime_float) {
                    // For floating point values, convert to i32 with proper rounding
                    const int_value: i32 = @intFromFloat(value);
                    mask[i] = evaluateIntCondition(item, op, int_value);
                } else if (@TypeOf(value) == i32) {
                    mask[i] = evaluateIntCondition(item, op, value);
                } else if (@TypeOf(value) == f32 or @TypeOf(value) == f64) {
                    const int_value: i32 = @intFromFloat(value);
                    mask[i] = evaluateIntCondition(item, op, int_value);
                } else {
                    mask[i] = false; // Incompatible types
                }
            }
        },
        .nullable_int => {
            const data = @as([*]?i32, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                if (op == .IsNull) {
                    mask[i] = item == null;
                } else if (op == .IsNotNull) {
                    mask[i] = item != null;
                } else if (item) |val| {
                    // Handle different numeric types for comparison
                    if (@TypeOf(value) == comptime_int) {
                        const int_value: i32 = @intCast(value);
                        mask[i] = evaluateIntCondition(val, op, int_value);
                    } else if (@TypeOf(value) == comptime_float) {
                        // For floating point values, convert to i32 with proper rounding
                        const int_value: i32 = @intFromFloat(value);
                        mask[i] = evaluateIntCondition(val, op, int_value);
                    } else if (@TypeOf(value) == i32) {
                        mask[i] = evaluateIntCondition(val, op, value);
                    } else if (@TypeOf(value) == f32 or @TypeOf(value) == f64) {
                        const int_value: i32 = @intFromFloat(value);
                        mask[i] = evaluateIntCondition(val, op, int_value);
                    } else {
                        mask[i] = false; // Incompatible types
                    }
                } else {
                    mask[i] = false;
                }
            }
        },
        .float => {
            const data = @as([*]f32, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                // Handle different numeric types for comparison
                if (@TypeOf(value) == comptime_float) {
                    const float_value: f32 = @floatCast(value);
                    mask[i] = evaluateFloatCondition(item, op, float_value);
                } else if (@TypeOf(value) == comptime_int) {
                    const float_value: f32 = @floatFromInt(value);
                    mask[i] = evaluateFloatCondition(item, op, float_value);
                } else if (@TypeOf(value) == f32) {
                    mask[i] = evaluateFloatCondition(item, op, value);
                } else if (@TypeOf(value) == f64) {
                    const float_value: f32 = @floatCast(value);
                    mask[i] = evaluateFloatCondition(item, op, float_value);
                } else if (@TypeOf(value) == i32) {
                    const float_value: f32 = @floatFromInt(value);
                    mask[i] = evaluateFloatCondition(item, op, float_value);
                } else {
                    mask[i] = false; // Incompatible types
                }
            }
        },
        .nullable_float => {
            const data = @as([*]?f32, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                if (op == .IsNull) {
                    mask[i] = item == null;
                } else if (op == .IsNotNull) {
                    mask[i] = item != null;
                } else if (item) |val| {
                    // Handle different numeric types for comparison
                    if (@TypeOf(value) == comptime_float) {
                        const float_value: f32 = @floatCast(value);
                        mask[i] = evaluateFloatCondition(val, op, float_value);
                    } else if (@TypeOf(value) == comptime_int) {
                        const float_value: f32 = @floatFromInt(value);
                        mask[i] = evaluateFloatCondition(val, op, float_value);
                    } else if (@TypeOf(value) == f32) {
                        mask[i] = evaluateFloatCondition(val, op, value);
                    } else if (@TypeOf(value) == f64) {
                        const float_value: f32 = @floatCast(value);
                        mask[i] = evaluateFloatCondition(val, op, float_value);
                    } else if (@TypeOf(value) == i32) {
                        const float_value: f32 = @floatFromInt(value);
                        mask[i] = evaluateFloatCondition(val, op, float_value);
                    } else {
                        mask[i] = false; // Incompatible types
                    }
                }
            }
        },
        .string => {
            const data = @as([*][]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                // Handle different string-like types
                if (@TypeOf(value) == []const u8) {
                    // Direct string comparison
                    mask[i] = evaluateStringCondition(item, op, value);
                } else if (@TypeOf(value) == *const [0:0]u8) {
                    // String literal (e.g., "Engineering")
                    const str_value: []const u8 = std.mem.span(value);
                    mask[i] = evaluateStringCondition(item, op, str_value);
                } else if (@TypeOf(value) == comptime_int) {
                    // For comptime_int, convert to string for comparison
                    var buf: [20]u8 = undefined;
                    const str = std.fmt.bufPrint(&buf, "{d}", .{value}) catch continue;
                    mask[i] = evaluateStringCondition(item, op, str);
                } else {
                    // Try to convert to string using std.fmt
                    var buf: [256]u8 = undefined;
                    const str = std.fmt.bufPrint(&buf, "{any}", .{value}) catch continue;
                    mask[i] = evaluateStringCondition(item, op, str);
                }
            }
        },
        .nullable_string => {
            const data = @as([*]?[]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                if (op == .IsNull) {
                    mask[i] = item == null;
                } else if (op == .IsNotNull) {
                    mask[i] = item != null;
                } else if (item) |str| {
                    // Handle different string-like types
                    if (@TypeOf(value) == []const u8) {
                        mask[i] = evaluateStringCondition(str, op, value);
                    } else if (@TypeOf(value) == *const [0:0]u8) {
                        const str_value: []const u8 = std.mem.span(value);
                        mask[i] = evaluateStringCondition(str, op, str_value);
                    } else if (@TypeOf(value) == comptime_int) {
                        var buf: [20]u8 = undefined;
                        const str_value = std.fmt.bufPrint(&buf, "{d}", .{value}) catch continue;
                        mask[i] = evaluateStringCondition(str, op, str_value);
                    } else {
                        // Try to convert to string using std.fmt
                        var buf: [256]u8 = undefined;
                        const str_value = std.fmt.bufPrint(&buf, "{any}", .{value}) catch continue;
                        mask[i] = evaluateStringCondition(str, op, str_value);
                    }
                } else {
                    mask[i] = false;
                }
            }
        },
        .boolean => {
            const data = @as([*]bool, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                // Convert value to bool if needed
                if (@TypeOf(value) == bool) {
                    mask[i] = evaluateBoolCondition(item, op, value);
                } else if (@TypeOf(value) == comptime_int) {
                    // For comptime_int, convert to bool (0 = false, non-0 = true)
                    const bool_value = value != 0;
                    mask[i] = evaluateBoolCondition(item, op, bool_value);
                } else {
                    mask[i] = false; // Incompatible types
                }
            }
        },
        .nullable_boolean => {
            const data = @as([*]?bool, @ptrCast(@alignCast(column.data_ptr)))[0..dataset.rows];
            for (data, 0..) |item, i| {
                if (op == .IsNull) {
                    mask[i] = item == null;
                } else if (op == .IsNotNull) {
                    mask[i] = item != null;
                } else if (item) |val| {
                    if (@TypeOf(value) == ?bool) {
                        mask[i] = evaluateBoolCondition(val, op, value.?);
                    } else if (@TypeOf(value) == bool) {
                        mask[i] = evaluateBoolCondition(val, op, value);
                    } else if (@TypeOf(value) == comptime_int) {
                        // For comptime_int, convert to bool (0 = false, non-0 = true)
                        const bool_value = value != 0;
                        mask[i] = evaluateBoolCondition(val, op, bool_value);
                    } else {
                        mask[i] = false; // Incompatible types
                    }
                } else {
                    mask[i] = false;
                }
            }
        },
        else => return error.UnsupportedType,
    }

    // Count matching rows
    var match_count: usize = 0;
    for (mask) |m| {
        if (m) match_count += 1;
    }

    // If no matches, return empty dataset with same schema
    if (match_count == 0) {
        // Create empty columns with the same schema
        for (dataset.columns.items) |col| {
            try createEmptyColumn(&result, col.name, col.type_id);
        }
        return result;
    }

    // Create filtered dataset by copying matching rows
    for (dataset.columns.items) |col| {
        try copyFilteredColumn(dataset, &result, col, mask, match_count);
    }

    return result;
}

/// Filter a dataset using a custom predicate function
pub fn filterWithPredicate(dataset: *const Dataset, predicate: PredicateFn) !Dataset {
    var result = Dataset.init(dataset.allocator);
    errdefer result.deinit();

    // Create a mask of rows that match the predicate
    var mask = try dataset.allocator.alloc(bool, dataset.rows);
    defer dataset.allocator.free(mask);

    // Apply the predicate to each row
    for (0..dataset.rows) |i| {
        mask[i] = predicate(i, dataset);
    }

    // Count matching rows
    var match_count: usize = 0;
    for (mask) |m| {
        if (m) match_count += 1;
    }

    // If no matches, return empty dataset with same schema
    if (match_count == 0) {
        // Create empty columns with the same schema
        for (dataset.columns.items) |col| {
            try createEmptyColumn(&result, col.name, col.type_id);
        }
        return result;
    }

    // Create filtered dataset by copying matching rows
    for (dataset.columns.items) |col| {
        try copyFilteredColumn(dataset, &result, col, mask, match_count);
    }

    return result;
}

/// Combine two datasets with logical AND operation
pub fn logicalAnd(dataset1: *const Dataset, dataset2: *const Dataset) !Dataset {
    // Special case: if either dataset is empty, return an empty dataset with the same structure as dataset1
    if (dataset1.rows == 0 or dataset2.rows == 0) {
        var result = Dataset.init(dataset1.allocator);
        // Copy column definitions from dataset1
        for (dataset1.columns.items) |col| {
            try result.addColumn(col.name, col.type_id);
        }
        return result;
    }
    
    // Normal case: check that datasets have the same number of rows
    if (dataset1.rows != dataset2.rows) {
        return error.LengthMismatch;
    }

    var result = Dataset.init(dataset1.allocator);
    errdefer result.deinit();

    // Create a mask that is true only where both datasets have rows
    const mask = try dataset1.allocator.alloc(bool, dataset1.rows);
    defer dataset1.allocator.free(mask);

    for (0..dataset1.rows) |i| {
        mask[i] = true; // All rows from dataset1 are included
    }

    // Count matching rows
    const match_count: usize = dataset1.rows;

    // Create filtered dataset by copying matching rows
    for (dataset1.columns.items) |col| {
        try copyFilteredColumn(dataset1, &result, col, mask, match_count);
    }

    return result;
}

/// Combine two datasets with logical OR operation
pub fn logicalOr(dataset1: *const Dataset, dataset2: *const Dataset) !Dataset {
    var result = Dataset.init(dataset1.allocator);
    errdefer result.deinit();

    // First, copy all rows from dataset1
    for (dataset1.columns.items) |col| {
        try copyEntireColumn(dataset1, &result, col);
    }

    // Then, add any columns from dataset2 that don't exist in dataset1
    for (dataset2.columns.items) |col2| {
        var exists = false;
        for (dataset1.columns.items) |col1| {
            if (std.mem.eql(u8, col1.name, col2.name)) {
                exists = true;
                break;
            }
        }

        if (!exists) {
            try copyEntireColumn(dataset2, &result, col2);
        }
    }

    return result;
}

// Helper functions for evaluating conditions

fn evaluateIntCondition(value: i32, op: ComparisonOp, target: i32) bool {
    return switch (op) {
        .Equal => value == target,
        .NotEqual => value != target,
        .GreaterThan => value > target,
        .GreaterThanOrEqual => value >= target,
        .LessThan => value < target,
        .LessThanOrEqual => value <= target,
        else => false, // Other operators don't apply to integers
    };
}

fn evaluateFloatCondition(value: f32, op: ComparisonOp, target: f32) bool {
    return switch (op) {
        .Equal => value == target,
        .NotEqual => value != target,
        .GreaterThan => value > target,
        .GreaterThanOrEqual => value >= target,
        .LessThan => value < target,
        .LessThanOrEqual => value <= target,
        else => false, // Other operators don't apply to floats
    };
}

/// Evaluate a string condition
fn evaluateStringCondition(str: []const u8, op: ComparisonOp, value: []const u8) bool {
    // Remove quotes if they exist (common in CSV data)
    var clean_str = str;
    if (clean_str.len >= 2 and clean_str[0] == '"' and clean_str[clean_str.len - 1] == '"') {
        clean_str = clean_str[1 .. clean_str.len - 1];
    }
    
    return switch (op) {
        .Equal => std.mem.eql(u8, clean_str, value),
        .NotEqual => !std.mem.eql(u8, clean_str, value),
        .Contains => std.mem.indexOf(u8, clean_str, value) != null,
        .StartsWith => std.mem.startsWith(u8, clean_str, value),
        .EndsWith => std.mem.endsWith(u8, clean_str, value),
        else => false, // Other operations don't apply to strings
    };
}

fn evaluateBoolCondition(value: bool, op: ComparisonOp, target: bool) bool {
    return switch (op) {
        .Equal => value == target,
        .NotEqual => value != target,
        else => false, // Other operators don't apply to booleans
    };
}

// Helper functions for copying data

fn createEmptyColumn(result: *Dataset, name: []const u8, type_id: TypeId) !void {
    const name_copy = try result.allocator.dupe(u8, name);
    errdefer result.allocator.free(name_copy);

    try result.columns.append(.{
        .name = name_copy,
        .type_id = type_id,
        .data_ptr = undefined,
    });
    result.column_count += 1;
}

fn copyFilteredColumn(source: *const Dataset, dest: *Dataset, column: types.ColumnMeta, mask: []const bool, match_count: usize) !void {
    const name_copy = try dest.allocator.dupe(u8, column.name);
    errdefer dest.allocator.free(name_copy);

    switch (column.type_id) {
        .int => {
            const data = @as([*]i32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(i32, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = item;
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .nullable_int => {
            const data = @as([*]?i32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(?i32, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = item;
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .float => {
            const data = @as([*]f32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(f32, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = item;
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .nullable_float => {
            const data = @as([*]?f32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(?f32, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = item;
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .string => {
            const data = @as([*][]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc([]const u8, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = try dest.allocator.dupe(u8, item);
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .nullable_string => {
            const data = @as([*]?[]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(?[]const u8, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    if (item) |str| {
                        filtered[j] = try dest.allocator.dupe(u8, str);
                    } else {
                        filtered[j] = null;
                    }
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .boolean => {
            const data = @as([*]bool, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(bool, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = item;
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        .nullable_boolean => {
            const data = @as([*]?bool, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var filtered = try dest.allocator.alloc(?bool, match_count);
            errdefer dest.allocator.free(filtered);

            var j: usize = 0;
            for (data, 0..) |item, i| {
                if (mask[i]) {
                    filtered[j] = item;
                    j += 1;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(filtered.ptr),
            });
        },
        else => return error.UnsupportedType,
    }

    dest.column_count += 1;
    if (dest.rows == 0) {
        dest.rows = match_count;
        dest.row_count = match_count;
    }
}

fn copyEntireColumn(source: *const Dataset, dest: *Dataset, column: types.ColumnMeta) !void {
    const name_copy = try dest.allocator.dupe(u8, column.name);
    errdefer dest.allocator.free(name_copy);

    switch (column.type_id) {
        .int => {
            const data = @as([*]i32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(i32, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = item;
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .nullable_int => {
            const data = @as([*]?i32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(?i32, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = item;
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .float => {
            const data = @as([*]f32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(f32, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = item;
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .nullable_float => {
            const data = @as([*]?f32, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(?f32, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = item;
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .string => {
            const data = @as([*][]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc([]const u8, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = try dest.allocator.dupe(u8, item);
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .nullable_string => {
            const data = @as([*]?[]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(?[]const u8, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                if (item) |str| {
                    copy[i] = try dest.allocator.dupe(u8, str);
                } else {
                    copy[i] = null;
                }
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .boolean => {
            const data = @as([*]bool, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(bool, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = item;
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        .nullable_boolean => {
            const data = @as([*]?bool, @ptrCast(@alignCast(column.data_ptr)))[0..source.rows];
            var copy = try dest.allocator.alloc(?bool, source.rows);
            errdefer dest.allocator.free(copy);

            for (data, 0..) |item, i| {
                copy[i] = item;
            }

            try dest.columns.append(.{
                .name = name_copy,
                .type_id = column.type_id,
                .data_ptr = @ptrCast(copy.ptr),
            });
        },
        else => return error.UnsupportedType,
    }

    dest.column_count += 1;
    if (dest.rows == 0) {
        dest.rows = source.rows;
        dest.row_count = source.rows;
    }
}
