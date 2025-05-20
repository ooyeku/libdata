const std = @import("std");
const types = @import("types.zig");
const TypeId = types.TypeId;
const ColumnMeta = types.ColumnMeta;
const Dataset = @import("core.zig").Dataset;

/// Formats a dataset for display
pub fn format(
    dataset: *Dataset,
    comptime fmt: []const u8,
    options: std.fmt.FormatOptions,
    writer: anytype,
) !void {
    _ = fmt;
    _ = options;
    try writer.print("Dataset with {d} rows and {d} columns\n", .{ dataset.row_count, dataset.column_count });

    for (dataset.columns.items) |col| {
        try writer.print("Column: {s}, Type: {s}, Values: ", .{ col.name, @tagName(col.type_id) });

        switch (col.type_id) {
            .nullable_int => {
                const data = @as([*]?i32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                try writer.print("{{ ", .{});
                for (data, 0..) |value, i| {
                    if (i > 0) try writer.print(", ", .{});
                    if (value) |v| {
                        try writer.print("{d}", .{v});
                    } else {
                        try writer.print("null", .{});
                    }
                }
                try writer.print(" }}", .{});
            },
            .nullable_float, .nullable_string, .nullable_char, .nullable_boolean => {
                try writer.print("<nullable type>", .{});
            },
            .int, .float, .string, .char, .boolean => {
                // ... existing non-nullable cases ...
            },
            .tensor, .image, .audio, .video, .text, .other => {
                try writer.print("<unsupported type>", .{});
            },
        }
        try writer.print("\n", .{});
    }
}

/// Formats a dataset as a table
pub fn formatAsTable(dataset: *Dataset, writer: anytype) !void {
    // First pass: calculate column widths
    var col_widths = try dataset.allocator.alloc(usize, dataset.column_count);
    defer dataset.allocator.free(col_widths);

    // Initialize with column name lengths
    for (dataset.columns.items, 0..) |col, i| {
        col_widths[i] = col.name.len;
    }

    // Check data lengths and update widths if necessary
    for (dataset.columns.items, 0..) |col, i| {
        switch (col.type_id) {
            .int => {
                const data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |value| {
                    var buf: [20]u8 = undefined;
                    const written = std.fmt.formatIntBuf(&buf, value, 10, .lower, .{});
                    col_widths[i] = @max(col_widths[i], written);
                }
            },
            .float => {
                const data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |value| {
                    var buf: [20]u8 = undefined;
                    if (std.fmt.bufPrint(&buf, "{d:.2}", .{value})) |result| {
                        col_widths[i] = @max(col_widths[i], result.len);
                    } else |_| {}
                }
            },
            .string => {
                const data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |str| {
                    col_widths[i] = @max(col_widths[i], str.len);
                }
            },
            .char => {
                // For char columns, width should account for padding
                col_widths[i] = @max(col_widths[i], 3); // At least 3 for single char + padding
            },
            .boolean => {
                const data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |value| {
                    const str = if (value) "true" else "false";
                    col_widths[i] = @max(col_widths[i], str.len);
                }
            },
            .nullable_int => {
                const data = @as([*]?i32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |maybe_value| {
                    if (maybe_value) |value| {
                        var buf: [20]u8 = undefined;
                        const written = std.fmt.formatIntBuf(&buf, value, 10, .lower, .{});
                        col_widths[i] = @max(col_widths[i], written);
                    } else {
                        col_widths[i] = @max(col_widths[i], 4); // "NULL"
                    }
                }
            },
            .nullable_float => {
                const data = @as([*]?f32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |maybe_value| {
                    if (maybe_value) |value| {
                        var buf: [20]u8 = undefined;
                        if (std.fmt.bufPrint(&buf, "{d:.2}", .{value})) |result| {
                            col_widths[i] = @max(col_widths[i], result.len);
                        } else |_| {}
                    } else {
                        col_widths[i] = @max(col_widths[i], 4); // "NULL"
                    }
                }
            },
            .nullable_string => {
                const data = @as([*]?[]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |maybe_str| {
                    if (maybe_str) |str| {
                        col_widths[i] = @max(col_widths[i], str.len);
                    } else {
                        col_widths[i] = @max(col_widths[i], 4); // "NULL"
                    }
                }
            },
            .nullable_char => {
                // For nullable char columns, width should account for NULL string
                col_widths[i] = @max(col_widths[i], 4); // "NULL"
            },
            .nullable_boolean => {
                const data = @as([*]?bool, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                for (data) |maybe_value| {
                    if (maybe_value) |value| {
                        const str = if (value) "true" else "false";
                        col_widths[i] = @max(col_widths[i], str.len);
                    } else {
                        col_widths[i] = @max(col_widths[i], 4); // "NULL"
                    }
                }
            },
            else => {},
        }
    }
    try printHeader(writer, col_widths);
    try printColumnNames(writer, col_widths, dataset.columns.items);
    try printSeparator(writer, col_widths);

    var row: usize = 0;
    while (row < dataset.rows) : (row += 1) {
        try printDataRow(writer, col_widths, row, dataset.columns.items, dataset.rows);
    }
    try printBottomBorder(writer, col_widths);
}

/// Simple plain text printer that's safer than formatAsTable for imported CSV data
pub fn printPlain(dataset: *Dataset, writer: anytype) !void {
    // Print header row
    for (dataset.columns.items, 0..) |col, i| {
        if (i > 0) try writer.writeAll(", ");
        try writer.writeAll(col.name);
    }
    try writer.writeAll("\n");
    
    // Print data rows
    var row: usize = 0;
    while (row < dataset.rows) : (row += 1) {
        for (dataset.columns.items, 0..) |col, i| {
            if (i > 0) try writer.writeAll(", ");
            
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
                    try writer.writeAll(data[row]);
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
                    } else {
                        try writer.writeAll("NULL");
                    }
                },
                .nullable_float => {
                    const data = @as([*]?f32, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.print("{d:.2}", .{value});
                    } else {
                        try writer.writeAll("NULL");
                    }
                },
                .nullable_string => {
                    const data = @as([*]?[]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.writeAll(value);
                    } else {
                        try writer.writeAll("NULL");
                    }
                },
                .nullable_char => {
                    const data = @as([*]?u8, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.writeByte(value);
                    } else {
                        try writer.writeAll("NULL");
                    }
                },
                .nullable_boolean => {
                    const data = @as([*]?bool, @ptrCast(@alignCast(col.data_ptr)))[0..dataset.rows];
                    if (data[row]) |value| {
                        try writer.writeAll(if (value) "true" else "false");
                    } else {
                        try writer.writeAll("NULL");
                    }
                },
                else => try writer.writeAll("<unsupported>"),
            }
        }
        try writer.writeAll("\n");
    }
}

// Helper functions for table formatting
fn printHeader(writer: anytype, col_widths: []usize) !void {
    try writer.writeAll("+");
    for (col_widths) |width| {
        try writer.writeByteNTimes('-', width + 2);
        try writer.writeAll("+");
    }
    try writer.writeAll("\n");
}

fn printColumnNames(writer: anytype, col_widths: []usize, columns: []ColumnMeta) !void {
    try writer.writeAll("|");
    for (columns, 0..) |col, i| {
        try writer.writeAll(" ");
        try writer.writeAll(col.name);
        const padding = if (col_widths[i] >= col.name.len)
            col_widths[i] - col.name.len + 1
        else
            1;
        try writer.writeByteNTimes(' ', padding);
        try writer.writeAll("|");
    }
    try writer.writeAll("\n");
}

fn printSeparator(writer: anytype, col_widths: []usize) !void {
    try writer.writeAll("+");
    for (col_widths) |width| {
        try writer.writeByteNTimes('-', width + 2);
        try writer.writeAll("+");
    }
    try writer.writeAll("\n");
}

fn printDataRow(writer: anytype, col_widths: []usize, row: usize, columns: []ColumnMeta, rows: usize) !void {
    try writer.writeAll("|");
    for (columns, 0..) |col, i| {
        try writer.writeAll(" ");
        switch (col.type_id) {
            .int => {
                const data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                var buf: [20]u8 = undefined;
                const written = std.fmt.formatIntBuf(&buf, data[row], 10, .lower, .{});
                try writer.writeAll(buf[0..written]);
                try writer.writeByteNTimes(' ', col_widths[i] - written + 1);
            },
            .float => {
                const data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                var buf: [20]u8 = undefined;
                if (std.fmt.bufPrint(&buf, "{d:.2}", .{data[row]}) catch null) |result| {
                    try writer.writeAll(result);
                    try writer.writeByteNTimes(' ', col_widths[i] - result.len + 1);
                }
            },
            .string => {
                const data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                try writer.writeAll(data[row]);
                try writer.writeByteNTimes(' ', col_widths[i] - data[row].len + 1);
            },
            .char => {
                const data = @as([*]u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                try writer.writeByte(data[row]);
                try writer.writeByteNTimes(' ', col_widths[i]);
            },
            .boolean => {
                const data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                const str = if (data[row]) "true" else "false";
                try writer.writeAll(str);
                try writer.writeByteNTimes(' ', col_widths[i] - str.len + 1);
            },
            .nullable_boolean => {
                const data = @as([*]?bool, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                const value = data[row];
                if (value) |v| {
                    const str = if (v) "true" else "false";
                    try writer.writeAll(str);
                    const padding = if (col_widths[i] > str.len)
                        col_widths[i] - str.len + 1
                    else
                        1;
                    try writer.writeByteNTimes(' ', padding);
                } else {
                    try writer.writeAll("NULL");
                    const padding = if (col_widths[i] > 4)
                        col_widths[i] - 4 + 1
                    else
                        1;
                    try writer.writeByteNTimes(' ', padding);
                }
            },
            .nullable_int => {
                const data = @as([*]?i32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                const value = data[row];
                if (value) |v| {
                    var buf: [20]u8 = undefined;
                    const written = std.fmt.formatIntBuf(&buf, v, 10, .lower, .{});
                    try writer.writeAll(buf[0..written]);
                    try writer.writeByteNTimes(' ', col_widths[i] - written + 1);
                } else {
                    try writer.writeAll("NULL");
                    const padding = if (col_widths[i] > 4)
                        col_widths[i] - 4 + 1
                    else
                        1;
                    try writer.writeByteNTimes(' ', padding);
                }
            },
            .nullable_float => {
                const data = @as([*]?f32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                const value = data[row];
                if (value) |v| {
                    var buf: [20]u8 = undefined;
                    if (std.fmt.bufPrint(&buf, "{d:.2}", .{v}) catch null) |result| {
                        try writer.writeAll(result);
                        try writer.writeByteNTimes(' ', col_widths[i] - result.len + 1);
                    }
                } else {
                    try writer.writeAll("NULL");
                    const padding = if (col_widths[i] > 4)
                        col_widths[i] - 4 + 1
                    else
                        1;
                    try writer.writeByteNTimes(' ', padding);
                }
            },
            .nullable_string => {
                const data = @as([*]?[]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                const value = data[row];
                if (value) |v| {
                    try writer.writeAll(v);
                    try writer.writeByteNTimes(' ', col_widths[i] - v.len + 1);
                } else {
                    try writer.writeAll("NULL");
                    const padding = if (col_widths[i] > 4)
                        col_widths[i] - 4 + 1
                    else
                        1;
                    try writer.writeByteNTimes(' ', padding);
                }
            },
            .nullable_char => {
                const data = @as([*]?u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
                const value = data[row];
                if (value) |v| {
                    try writer.writeByte(v);
                    try writer.writeByteNTimes(' ', col_widths[i]);
                } else {
                    try writer.writeAll("NULL");
                    const padding = if (col_widths[i] > 4)
                        col_widths[i] - 4 + 1
                    else
                        1;
                    try writer.writeByteNTimes(' ', padding);
                }
            },
            else => try writer.writeByteNTimes(' ', col_widths[i] + 2),
        }
        try writer.writeAll("|");
    }
    try writer.writeAll("\n");
}

fn printBottomBorder(writer: anytype, col_widths: []usize) !void {
    try writer.writeAll("+");
    for (col_widths) |width| {
        try writer.writeByteNTimes('-', width + 2);
        try writer.writeAll("+");
    }
    try writer.writeAll("\n");
}
