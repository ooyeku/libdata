const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const TypeId = types.TypeId;
const Dataset = @import("core.zig").Dataset;
const DatasetError = types.DatasetError;

/// Export a dataset to a CSV file
pub fn toCSV(dataset: *Dataset, path: []const u8) !void {
    var file = try std.fs.cwd().createFile(path, .{});
    defer file.close();
    const writer = file.writer();

    // Write header row
    for (dataset.columns.items, 0..) |col, i| {
        if (i > 0) try writer.writeAll(",");
        try writer.writeAll(col.name);
    }
    try writer.writeAll("\n");

    // Write data rows
    var row: usize = 0;
    while (row < dataset.rows) : (row += 1) {
        for (dataset.columns.items, 0..) |col, i| {
            if (i > 0) try writer.writeAll(",");

            switch (col.type_id) {
                .int => {
                    const data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    try writer.print("{d}", .{data[row]});
                },
                .float => {
                    const data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    try writer.print("{d:.2}", .{data[row]});
                },
                .string => {
                    const data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    // Escape quotes and wrap in quotes if contains comma
                    const str = data[row];
                    if (std.mem.indexOf(u8, str, ",") != null) {
                        try writer.writeByte('"');
                        for (str) |c| {
                            if (c == '"') try writer.writeAll("\"\"");
                            try writer.writeByte(c);
                        }
                        try writer.writeByte('"');
                    } else {
                        try writer.writeAll(str);
                    }
                },
                .char => {
                    const data = @as([*]u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    try writer.writeByte(data[row]);
                },
                .boolean => {
                    const data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    try writer.writeAll(if (data[row]) "true" else "false");
                },
                .nullable_int => {
                    const data = @as([*]?i32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.print("{d}", .{value});
                    }
                },
                .nullable_float => {
                    const data = @as([*]?f32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.print("{d:.2}", .{value});
                    }
                },
                .nullable_string => {
                    const data = @as([*]?[]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |str| {
                        if (std.mem.indexOf(u8, str, ",") != null) {
                            try writer.writeByte('"');
                            for (str) |c| {
                                if (c == '"') try writer.writeAll("\"\"");
                                try writer.writeByte(c);
                            }
                            try writer.writeByte('"');
                        } else {
                            try writer.writeAll(str);
                        }
                    }
                },
                .nullable_char => {
                    const data = @as([*]?u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |c| {
                        try writer.writeByte(c);
                    }
                },
                .nullable_boolean => {
                    const data = @as([*]?bool, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.writeAll(if (value) "true" else "false");
                    }
                },
                else => {},
            }
        }
        try writer.writeAll("\n");
    }
}

/// Import a dataset from a CSV file
pub fn fromCSV(dataset: *Dataset, path: []const u8) !void {
    // Read entire file
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const file_content = try file.readToEndAlloc(dataset.allocator, std.math.maxInt(usize));
    defer dataset.allocator.free(file_content);

    // Split into lines
    var lines = std.mem.tokenizeScalar(u8, file_content, '\n');

    // Parse header row
    const header = lines.next() orelse return error.EmptyFile;
    var header_iter = std.mem.tokenizeScalar(u8, header, ',');

    // Count columns and store names
    var column_names = std.ArrayList([]const u8).init(dataset.allocator);
    defer {
        for (column_names.items) |name| {
            dataset.allocator.free(name);
        }
        column_names.deinit();
    }
    
    while (header_iter.next()) |name| {
        const name_copy = try dataset.allocator.dupe(u8, std.mem.trim(u8, name, " "));
        try column_names.append(name_copy);
    }

    // Count rows
    var row_count: usize = 0;
    var lines_iter = lines;
    while (lines_iter.next()) |_| {
        row_count += 1;
    }
    
    if (row_count == 0) {
        return;  // No data rows
    }

    // Collect all data as strings first
    var data_rows = try dataset.allocator.alloc([][]const u8, row_count);
    defer {
        for (data_rows) |row_data| {
            for (row_data) |value| {
                dataset.allocator.free(value);
            }
            dataset.allocator.free(row_data);
        }
        dataset.allocator.free(data_rows);
    }
    
    // Reset the lines iterator to parse data rows
    lines = std.mem.tokenizeScalar(u8, file_content, '\n');
    _ = lines.next(); // Skip header
    
    // Parse each row into string columns
    var row_idx: usize = 0;
    while (lines.next()) |line| : (row_idx += 1) {
        var values = std.mem.tokenizeScalar(u8, line, ',');
        
        // Allocate memory for this row's data
        var row_data = try dataset.allocator.alloc([]const u8, column_names.items.len);
        data_rows[row_idx] = row_data;
        
        // Initialize all columns to empty strings
        for (row_data) |*value| {
            value.* = "";
        }
        
        var col_idx: usize = 0;
        while (values.next()) |value| : (col_idx += 1) {
            if (col_idx >= column_names.items.len) break;
            
            const trimmed = std.mem.trim(u8, value, " ");
            row_data[col_idx] = try dataset.allocator.dupe(u8, trimmed);
        }
    }
    
    // Set dataset dimensions
    dataset.rows = row_count;
    dataset.row_count = row_count;

    // Try to intelligently add typed columns
    for (column_names.items, 0..) |col_name, col_idx| {
        // First pass: detect if the column is all integers
        var is_all_int = true;
        var is_all_float = true;
        var is_all_bool = true;
        var is_all_single_char = true;
        var has_null = false;
        
        for (data_rows) |row_data| {
            const value = row_data[col_idx];
            
            // Check for NULL values
            if (std.mem.eql(u8, value, "NULL") or value.len == 0) {
                has_null = true;
                continue;
            }
            
            // Check if all values are single characters
            if (value.len != 1) {
                is_all_single_char = false;
            }
            
            // Check if all values are booleans
            if (!std.mem.eql(u8, value, "true") and !std.mem.eql(u8, value, "false")) {
                is_all_bool = false;
            }
            
            // Check if all values can be parsed as integers
            _ = std.fmt.parseInt(i32, value, 10) catch {
                is_all_int = false;
            };
            
            // Check if all values can be parsed as floats
            _ = std.fmt.parseFloat(f32, value) catch {
                is_all_float = false;
            };
        }
        
        // Create appropriate typed arrays based on detection
        if (is_all_int and !has_null) {
            // Create an integer array
            var int_data = try dataset.allocator.alloc(i32, row_count);
            defer dataset.allocator.free(int_data);
            
            for (data_rows, 0..) |row_data, i| {
                int_data[i] = std.fmt.parseInt(i32, row_data[col_idx], 10) catch 0;
            }
            
            try dataset.addColumn(col_name, int_data);
        } else if (is_all_int and has_null) {
            // Create a nullable integer array
            var nullable_int_data = try dataset.allocator.alloc(?i32, row_count);
            defer dataset.allocator.free(nullable_int_data);
            
            for (data_rows, 0..) |row_data, i| {
                const value = row_data[col_idx];
                if (std.mem.eql(u8, value, "NULL") or value.len == 0) {
                    nullable_int_data[i] = null;
                } else {
                    nullable_int_data[i] = std.fmt.parseInt(i32, value, 10) catch null;
                }
            }
            
            try dataset.addColumn(col_name, nullable_int_data);
        } else if (is_all_float and !has_null) {
            // Create a float array
            var float_data = try dataset.allocator.alloc(f32, row_count);
            defer dataset.allocator.free(float_data);
            
            for (data_rows, 0..) |row_data, i| {
                float_data[i] = std.fmt.parseFloat(f32, row_data[col_idx]) catch 0;
            }
            
            try dataset.addColumn(col_name, float_data);
        } else if (is_all_float and has_null) {
            // Create a nullable float array
            var nullable_float_data = try dataset.allocator.alloc(?f32, row_count);
            defer dataset.allocator.free(nullable_float_data);
            
            for (data_rows, 0..) |row_data, i| {
                const value = row_data[col_idx];
                if (std.mem.eql(u8, value, "NULL") or value.len == 0) {
                    nullable_float_data[i] = null;
                } else {
                    nullable_float_data[i] = std.fmt.parseFloat(f32, value) catch null;
                }
            }
            
            try dataset.addColumn(col_name, nullable_float_data);
        } else if (is_all_bool and !has_null) {
            // Create a boolean array
            var bool_data = try dataset.allocator.alloc(bool, row_count);
            defer dataset.allocator.free(bool_data);
            
            for (data_rows, 0..) |row_data, i| {
                bool_data[i] = std.mem.eql(u8, row_data[col_idx], "true");
            }
            
            try dataset.addColumn(col_name, bool_data);
        } else if (is_all_bool and has_null) {
            // Create a nullable boolean array
            var nullable_bool_data = try dataset.allocator.alloc(?bool, row_count);
            defer dataset.allocator.free(nullable_bool_data);
            
            for (data_rows, 0..) |row_data, i| {
                const value = row_data[col_idx];
                if (std.mem.eql(u8, value, "NULL") or value.len == 0) {
                    nullable_bool_data[i] = null;
                } else {
                    nullable_bool_data[i] = std.mem.eql(u8, value, "true");
                }
            }
            
            try dataset.addColumn(col_name, nullable_bool_data);
        } else if (is_all_single_char and !has_null) {
            // Create a char array
            var char_data = try dataset.allocator.alloc(u8, row_count);
            defer dataset.allocator.free(char_data);
            
            for (data_rows, 0..) |row_data, i| {
                const value = row_data[col_idx];
                char_data[i] = if (value.len > 0) value[0] else 0;
            }
            
            try dataset.addColumn(col_name, char_data);
        } else if (is_all_single_char and has_null) {
            // Create a nullable char array
            var nullable_char_data = try dataset.allocator.alloc(?u8, row_count);
            defer dataset.allocator.free(nullable_char_data);
            
            for (data_rows, 0..) |row_data, i| {
                const value = row_data[col_idx];
                if (std.mem.eql(u8, value, "NULL") or value.len == 0) {
                    nullable_char_data[i] = null;
                } else {
                    nullable_char_data[i] = if (value.len > 0) value[0] else null;
                }
            }
            
            try dataset.addColumn(col_name, nullable_char_data);
        } else {
            // Default to string data for everything else
            var string_data = try dataset.allocator.alloc([]const u8, row_count);
            defer dataset.allocator.free(string_data);
            
            for (data_rows, 0..) |row_data, i| {
                const value = row_data[col_idx];
                string_data[i] = try dataset.allocator.dupe(u8, value);
            }
            
            try dataset.addColumn(col_name, string_data);
        }
    }
}

/// Helper function to parse CSV header
fn parseCSVHeader(allocator: Allocator, header: []const u8) !std.ArrayList([]const u8) {
    var column_names = std.ArrayList([]const u8).init(allocator);
    var header_iter = std.mem.tokenizeScalar(u8, header, ',');

    while (header_iter.next()) |name| {
        const name_copy = try allocator.dupe(u8, std.mem.trim(u8, name, " "));
        try column_names.append(name_copy);
    }
    return column_names;
}

/// Helper function to parse CSV value
fn parseCSVValue(value: []const u8) !i32 {
    const trimmed = std.mem.trim(u8, value, " ");
    if (trimmed.len == 0) {
        return 0;
    }
    return std.fmt.parseInt(i32, trimmed, 10) catch |err| switch (err) {
        error.InvalidCharacter => 0,
        else => return err,
    };
}

/// Helper function to write CSV value
fn writeCSVValue(writer: anytype, value: anytype, type_id: TypeId) !void {
    switch (type_id) {
        .int => try writer.print("{d}", .{value}),
        .float => try writer.print("{d:.2}", .{value}),
        .string => {
            if (std.mem.indexOf(u8, value, ",") != null) {
                try writer.writeByte('"');
                for (value) |c| {
                    if (c == '"') try writer.writeAll("\"\"");
                    try writer.writeByte(c);
                }
                try writer.writeByte('"');
            } else {
                try writer.writeAll(value);
            }
        },
        .char => try writer.writeByte(value),
        .boolean => try writer.writeAll(if (value) "true" else "false"),
        .nullable_int, .nullable_float => if (value) |v| {
            try writer.print("{d}", .{v});
        },
        .nullable_string => if (value) |str| {
            try writeCSVValue(writer, str, .string);
        },
        .nullable_char => if (value) |c| {
            try writer.writeByte(c);
        },
        .nullable_boolean => if (value) |v| {
            try writer.writeAll(if (v) "true" else "false");
        },
        else => {},
    }
}
