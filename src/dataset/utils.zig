const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const core = @import("core.zig");

/// Infer the type of a string value
pub fn inferTypeFromString(value: []const u8) types.TypeId {
    // Try to parse as integer
    if (std.fmt.parseInt(i32, value, 10)) |_| {
        return .int;
    } else |_| {}
    
    // Try to parse as float
    if (std.fmt.parseFloat(f32, value)) |_| {
        return .float;
    } else |_| {}
    
    // Check if it's a boolean
    if (std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "false") or 
        std.mem.eql(u8, value, "1") or std.mem.eql(u8, value, "0")) {
        return .boolean;
    }
    
    // If it's a single character
    if (value.len == 1) {
        return .char;
    }
    
    // Default to string
    return .string;
}
const ColumnMeta = types.ColumnMeta;
const Dataset = core.Dataset;

/// Determines the TypeId for a given type
pub fn determineColumnType(comptime T: type) types.TypeId {
    if (T == ?i32 or T == ?u32) {
        return types.TypeId.nullable_int;
    } else if (T == ?f32) {
        return types.TypeId.nullable_float;
    } else if (T == ?[]const u8) {
        return types.TypeId.nullable_string;
    } else if (T == ?bool) {
        return types.TypeId.nullable_boolean;
    } else if (T == ?u8) {
        return types.TypeId.nullable_char;
    } else if (T == i32 or T == u32) {
        return types.TypeId.int;
    } else if (T == f32) {
        return types.TypeId.float;
    } else if (T == []const u8) {
        return types.TypeId.string;
    } else if (T == bool) {
        return types.TypeId.boolean;
    } else if (T == u8) {
        return types.TypeId.char;
    } else {
        return types.TypeId.unknown;
    }
}

/// Copy column data from source to destination dataset with filtering
pub fn copyColumnDataFiltered(src: *const Dataset, dst: *Dataset, col_idx: usize, mask: []const bool, matched_rows: usize) !void {
    
    const src_col = src.columns.items[col_idx];
    const dst_col_idx = dst.getColumnIndex(src_col.name) orelse return error.ColumnNotFound;
    
    // Allocate memory for the filtered data based on the column type
    switch (src_col.type_id) {
        .int => {
            const src_data = @as([*]i32, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(i32, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .nullable_int => {
            const src_data = @as([*]?i32, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(?i32, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .float => {
            const src_data = @as([*]f32, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(f32, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .nullable_float => {
            const src_data = @as([*]?f32, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(?f32, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .string => {
            const src_data = @as([*][]const u8, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc([]const u8, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = try dst.allocator.dupe(u8, val);
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .nullable_string => {
            const src_data = @as([*]?[]const u8, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(?[]const u8, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    if (val) |str| {
                        dst_data[dst_idx] = try dst.allocator.dupe(u8, str);
                    } else {
                        dst_data[dst_idx] = null;
                    }
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .boolean => {
            const src_data = @as([*]bool, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(bool, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .nullable_boolean => {
            const src_data = @as([*]?bool, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(?bool, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .char => {
            const src_data = @as([*]u8, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(u8, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        .nullable_char => {
            const src_data = @as([*]?u8, @ptrCast(@alignCast(src_col.data_ptr)))[0..src.rows];
            var dst_data = try dst.allocator.alloc(?u8, matched_rows);
            var dst_idx: usize = 0;
            
            for (src_data, 0..) |val, i| {
                if (mask[i]) {
                    dst_data[dst_idx] = val;
                    dst_idx += 1;
                }
            }
            
            dst.columns.items[dst_col_idx].data_ptr = @ptrCast(dst_data.ptr);
        },
        else => return error.UnsupportedType,
    }
    
    // Update row count in the destination dataset
    dst.rows = matched_rows;
    dst.row_count = matched_rows;
}

/// Frees the memory allocated for column data
pub fn freeColumnData(allocator: Allocator, col: ColumnMeta, rows: usize) void {
    switch (col.type_id) {
        .int => {
            const data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .float => {
            const data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .string => {
            const data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .char => {
            const data = @as([*]u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .boolean => {
            const data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .nullable_int => {
            const data = @as([*]?i32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .nullable_float => {
            const data = @as([*]?f32, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .nullable_string => {
            const data = @as([*]?[]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .nullable_char => {
            const data = @as([*]?u8, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .nullable_boolean => {
            const data = @as([*]?bool, @ptrCast(@alignCast(col.data_ptr)))[0..rows];
            allocator.free(data);
        },
        .tensor, .image, .audio, .video, .text, .other => {},
    }
}
