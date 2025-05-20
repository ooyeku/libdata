const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const utils = @import("utils.zig");
const fmt = @import("format.zig");
const csv = @import("csv.zig");
const stats = @import("stats.zig");
const filter_mod = @import("filter.zig");

const TypeId = types.TypeId;
const ColumnMeta = types.ColumnMeta;
const DatasetError = types.DatasetError;

/// Dataset is a container for tabular data with named columns
pub const Dataset = struct {
    columns: std.ArrayList(ColumnMeta),
    rows: usize,
    allocator: Allocator,
    row_count: usize,
    column_count: usize,

    pub fn init(allocator: Allocator) Dataset {
        return .{
            .columns = std.ArrayList(ColumnMeta).init(allocator),
            .rows = 0,
            .allocator = allocator,
            .row_count = 0,
            .column_count = 0,
        };
    }

    pub fn deinit(self: *Dataset) void {
        for (self.columns.items) |col| {
            self.allocator.free(col.name);
            utils.freeColumnData(self.allocator, col, self.rows);
        }
        self.columns.deinit();
    }

    /// Add a new column to the dataset
    pub fn addColumn(self: *Dataset, name: []const u8, data: anytype) !void {
        // Check if data is a TypeId (for empty column creation)
        if (@TypeOf(data) == types.TypeId) {
            // For empty columns, we need to create a placeholder pointer
            // Allocate a single empty element based on the type
            var empty_data: *anyopaque = undefined;
            
            switch (data) {
                .int => {
                    const ptr = try self.allocator.create(i32);
                    ptr.* = 0;
                    empty_data = @ptrCast(ptr);
                },
                .float => {
                    const ptr = try self.allocator.create(f32);
                    ptr.* = 0;
                    empty_data = @ptrCast(ptr);
                },
                .string => {
                    const ptr = try self.allocator.create([]const u8);
                    ptr.* = "";
                    empty_data = @ptrCast(ptr);
                },
                .boolean => {
                    const ptr = try self.allocator.create(bool);
                    ptr.* = false;
                    empty_data = @ptrCast(ptr);
                },
                else => {
                    // For other types, just use a void pointer
                    const ptr = try self.allocator.create(u8);
                    ptr.* = 0;
                    empty_data = @ptrCast(ptr);
                },
            }
            
            const col = ColumnMeta{
                .name = try self.allocator.dupe(u8, name),
                .type_id = data,
                .data_ptr = empty_data,
            };
            try self.columns.append(col);
            self.column_count += 1;
            return;
        }
        
        // For actual data arrays, get the element type
        const T = @TypeOf(data[0]);

        // Validate data length
        if (self.rows == 0) {
            self.rows = data.len;
            self.row_count = data.len;
        } else if (data.len != self.rows) {
            return error.LengthMismatch;
        }

        // Copy name and data
        const name_copy = try self.allocator.dupe(u8, name);
        const data_copy = try self.allocator.dupe(T, data);

        // Determine type
        const type_id = utils.determineColumnType(T);

        try self.columns.append(.{
            .name = name_copy,
            .data_ptr = @ptrCast(data_copy.ptr),
            .type_id = type_id,
        });
        self.column_count += 1;
    }

    pub fn getColumn(self: *const Dataset, name: []const u8, comptime T: type) DatasetError![]const T {
        for (self.columns.items) |col| {
            if (std.mem.eql(u8, col.name, name)) {
                const expected_type_id = switch (T) {
                    i32, u32 => TypeId.int,
                    f32 => TypeId.float,
                    []const u8 => TypeId.string,
                    ?i32 => TypeId.nullable_int,
                    ?f32 => TypeId.nullable_float,
                    ?[]const u8 => TypeId.nullable_string,
                    ?bool => TypeId.nullable_boolean,
                    else => @compileError("Unsupported type"),
                };

                if (col.type_id == expected_type_id) {
                    return @as([*]T, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                }
            }
        }
        return DatasetError.ColumnNotFound;
    }

    /// Format the dataset for display
    pub fn format(
        self: *Dataset,
        comptime format_str: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try fmt.format(self, format_str, options, writer);
    }

    /// Format the dataset as a table
    pub fn formatAsTable(self: *Dataset, writer: anytype) !void {
        try fmt.formatAsTable(self, writer);
    }

    /// Print the dataset to stdout
    pub fn print(self: *Dataset) !void {
        try self.formatAsTable(std.io.getStdOut().writer());
    }

    /// Print the dataset in a plain text format
    pub fn printPlain(self: *Dataset, writer: anytype) !void {
        try fmt.printPlain(self, writer);
    }

    /// Export the dataset to a CSV file
    pub fn toCSV(self: *Dataset, path: []const u8) !void {
        try csv.toCSV(self, path);
    }

    /// Import a dataset from a CSV file
    pub fn fromCSV(self: *Dataset, path: []const u8) !void {
        try csv.fromCSV(self, path);
    }

    /// Calculate the minimum value in a column
    pub fn columnMin(self: *Dataset, name: []const u8) !f32 {
        return stats.columnMin(self, name);
    }

    /// Calculate the maximum value in a column
    pub fn columnMax(self: *Dataset, name: []const u8) !f32 {
        return stats.columnMax(self, name);
    }

    /// Calculate the mean value of a column
    pub fn columnMean(self: *Dataset, name: []const u8) !f32 {
        return stats.columnMean(self, name);
    }

    /// Calculate the standard deviation of a column
    pub fn columnStdDev(self: *Dataset, name: []const u8) !f32 {
        return stats.columnStdDev(self, name);
    }

    /// Calculate the median value of a column
    pub fn columnMedian(self: *Dataset, name: []const u8) !f32 {
        return stats.columnMedian(self, name);
    }

    /// Calculate the mode (most frequent value) of a column
    pub fn columnMode(self: *Dataset, name: []const u8) !f32 {
        return stats.columnMode(self, name);
    }

    /// Calculate the range (max - min) of a column
    /// Add a new row to the dataset
    pub fn addRow(self: *Dataset, values: []const []const u8) !void {
        if (values.len != self.columns.items.len) {
            return error.LengthMismatch;
        }
        
        // For the first row, we need to infer types if columns are empty
        if (self.rows == 0) {
            for (self.columns.items, 0..) |*col, i| {
                const value = values[i];
                
                // Skip if type is already set (not string)
                if (col.type_id != .string) continue;
                
                // Try to infer type
                const inferred_type = utils.inferTypeFromString(value);
                
                // Initialize column data based on inferred type
                switch (inferred_type) {
                    .int => {
                        const data = try self.allocator.alloc(i32, 1);
                        data[0] = try std.fmt.parseInt(i32, value, 10);
                        
                        // Free old empty data if it exists
                        if (@intFromPtr(col.data_ptr) != 0) {
                            const old_ptr = @as(*i32, @ptrCast(@alignCast(col.data_ptr)));
                            self.allocator.destroy(old_ptr);
                        }
                        
                        col.data_ptr = @ptrCast(data.ptr);
                        col.type_id = .int;
                    },
                    .float => {
                        const data = try self.allocator.alloc(f32, 1);
                        data[0] = try std.fmt.parseFloat(f32, value);
                        
                        // Free old empty data if it exists
                        if (@intFromPtr(col.data_ptr) != 0) {
                            const old_ptr = @as(*f32, @ptrCast(@alignCast(col.data_ptr)));
                            self.allocator.destroy(old_ptr);
                        }
                        
                        col.data_ptr = @ptrCast(data.ptr);
                        col.type_id = .float;
                    },
                    .boolean => {
                        const data = try self.allocator.alloc(bool, 1);
                        data[0] = std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1");
                        
                        // Free old empty data if it exists
                        if (@intFromPtr(col.data_ptr) != 0) {
                            const old_ptr = @as(*bool, @ptrCast(@alignCast(col.data_ptr)));
                            self.allocator.destroy(old_ptr);
                        }
                        
                        col.data_ptr = @ptrCast(data.ptr);
                        col.type_id = .boolean;
                    },
                    .string => {
                        const data = try self.allocator.alloc([]const u8, 1);
                        data[0] = try self.allocator.dupe(u8, value);
                        
                        // Free old empty data if it exists
                        if (@intFromPtr(col.data_ptr) != 0) {
                            const old_ptr = @as(*[]const u8, @ptrCast(@alignCast(col.data_ptr)));
                            self.allocator.destroy(old_ptr);
                        }
                        
                        col.data_ptr = @ptrCast(data.ptr);
                        col.type_id = .string;
                    },
                    else => {
                        // Default to string for other types
                        const data = try self.allocator.alloc([]const u8, 1);
                        data[0] = try self.allocator.dupe(u8, value);
                        
                        // Free old empty data if it exists
                        if (@intFromPtr(col.data_ptr) != 0) {
                            const old_ptr = @as(*[]const u8, @ptrCast(@alignCast(col.data_ptr)));
                            self.allocator.destroy(old_ptr);
                        }
                        
                        col.data_ptr = @ptrCast(data.ptr);
                        col.type_id = .string;
                    },
                }
            }
            self.rows = 1;
            self.row_count = 1;
            return;
        }
        
        // For subsequent rows, append to existing columns
        for (self.columns.items, 0..) |*col, i| {
            const value = values[i];
            
            switch (col.type_id) {
                .int => {
                    const old_data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                    var new_data = try self.allocator.alloc(i32, self.rows + 1);
                    @memcpy(new_data[0..self.rows], old_data);
                    new_data[self.rows] = try std.fmt.parseInt(i32, value, 10);
                    
                    self.allocator.free(old_data);
                    col.data_ptr = @ptrCast(new_data.ptr);
                },
                .float => {
                    const old_data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                    var new_data = try self.allocator.alloc(f32, self.rows + 1);
                    @memcpy(new_data[0..self.rows], old_data);
                    new_data[self.rows] = try std.fmt.parseFloat(f32, value);
                    
                    self.allocator.free(old_data);
                    col.data_ptr = @ptrCast(new_data.ptr);
                },
                .boolean => {
                    const old_data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                    var new_data = try self.allocator.alloc(bool, self.rows + 1);
                    @memcpy(new_data[0..self.rows], old_data);
                    new_data[self.rows] = std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1");
                    
                    self.allocator.free(old_data);
                    col.data_ptr = @ptrCast(new_data.ptr);
                },
                .string => {
                    const old_data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                    var new_data = try self.allocator.alloc([]const u8, self.rows + 1);
                    @memcpy(new_data[0..self.rows], old_data);
                    new_data[self.rows] = try self.allocator.dupe(u8, value);
                    
                    self.allocator.free(old_data);
                    col.data_ptr = @ptrCast(new_data.ptr);
                },
                .char => {
                    const old_data = @as([*]u8, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                    var new_data = try self.allocator.alloc(u8, self.rows + 1);
                    @memcpy(new_data[0..self.rows], old_data);
                    new_data[self.rows] = if (value.len > 0) value[0] else 0;
                    
                    self.allocator.free(old_data);
                    col.data_ptr = @ptrCast(new_data.ptr);
                },
                else => {
                    // For other types, try to convert to string
                    const old_data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..self.rows];
                    var new_data = try self.allocator.alloc([]const u8, self.rows + 1);
                    @memcpy(new_data[0..self.rows], old_data);
                    new_data[self.rows] = try self.allocator.dupe(u8, value);
                    
                    self.allocator.free(old_data);
                    col.data_ptr = @ptrCast(new_data.ptr);
                },
            }
        }
        
        self.rows += 1;
        self.row_count += 1;
    }
    
    /// Append data from a slice to an existing column
    pub fn appendColumnData(self: *Dataset, name: []const u8, data: anytype) !void {
        const T = @TypeOf(data[0]);
        
        for (self.columns.items) |*col| {
            if (std.mem.eql(u8, col.name, name)) {
                // Get existing data
                const old_size = self.rows;
                const new_size = old_size + data.len;
                
                switch (col.type_id) {
                    .int => {
                        // We'll handle type conversion as best we can
                        const old_data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..old_size];
                        var new_data = try self.allocator.alloc(i32, new_size);
                        
                        // Copy old data
                        @memcpy(new_data[0..old_size], old_data);
                        
                        // Copy new data with conversion
                        for (0..data.len) |i| {
                            // Try to convert to int based on type
                            new_data[old_size + i] = switch (@TypeOf(data[i])) {
                                i32 => data[i],
                                u32 => @intCast(data[i]),
                                f32 => @intFromFloat(data[i]),
                                bool => @intFromBool(data[i]),
                                else => 0, // Default for unsupported types
                            };
                        }
                        
                        // Free old data and update pointer
                        self.allocator.free(old_data);
                        col.data_ptr = @ptrCast(new_data.ptr);
                    },
                    .float => {
                        // We'll handle type conversion as best we can
                        const old_data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..old_size];
                        var new_data = try self.allocator.alloc(f32, new_size);
                        
                        // Copy old data
                        @memcpy(new_data[0..old_size], old_data);
                        
                        // Copy new data with conversion
                        for (0..data.len) |i| {
                            // Try to convert to float based on type
                            new_data[old_size + i] = switch (@TypeOf(data[i])) {
                                f32 => data[i],
                                i32, u32 => @floatFromInt(data[i]),
                                bool => if (data[i]) 1.0 else 0.0,
                                else => 0.0, // Default for unsupported types
                            };
                        }
                        
                        // Free old data and update pointer
                        self.allocator.free(old_data);
                        col.data_ptr = @ptrCast(new_data.ptr);
                    },
                    .boolean => {
                        // We'll handle type conversion as best we can
                        const old_data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..old_size];
                        var new_data = try self.allocator.alloc(bool, new_size);
                        
                        // Copy old data
                        @memcpy(new_data[0..old_size], old_data);
                        
                        // Copy new data with conversion
                        for (0..data.len) |i| {
                            // Try to convert to bool based on type
                            new_data[old_size + i] = switch (@TypeOf(data[i])) {
                                bool => data[i],
                                i32, u32 => data[i] != 0,
                                f32 => data[i] != 0.0,
                                []const u8 => std.mem.eql(u8, data[i], "true"),
                                else => false, // Default for unsupported types
                            };
                        }
                        
                        // Free old data and update pointer
                        self.allocator.free(old_data);
                        col.data_ptr = @ptrCast(new_data.ptr);
                    },
                    .string => {
                        // Special handling for string type - we need to convert any type to string
                        
                        const old_data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..old_size];
                        var new_data = try self.allocator.alloc([]const u8, new_size);
                        
                        // Copy old data
                        @memcpy(new_data[0..old_size], old_data);
                        
                        // Copy new data with duplication and conversion to string if needed
                        for (0..data.len) |i| {
                            var buf: [128]u8 = undefined;
                            const str = switch (@TypeOf(data[i])) {
                                []const u8 => data[i],
                                i32, u32 => blk: {
                                    const int_len = std.fmt.formatIntBuf(&buf, data[i], 10, .lower, .{});
                                    break :blk buf[0..int_len];
                                },
                                f32 => blk: {
                                    const float_str = std.fmt.bufPrint(&buf, "{d:.6}", .{data[i]}) catch buf[0..0];
                                    break :blk float_str;
                                },
                                bool => if (data[i]) "true" else "false",
                                else => "<unsupported type>",
                            };
                            new_data[old_size + i] = try self.allocator.dupe(u8, str);
                        }
                        
                        // Free old data and update pointer
                        self.allocator.free(old_data);
                        col.data_ptr = @ptrCast(new_data.ptr);
                    },
                    .char => {
                        if (@TypeOf(T) != u8) return error.TypeMismatch;
                        
                        const old_data = @as([*]u8, @ptrCast(@alignCast(col.data_ptr)))[0..old_size];
                        var new_data = try self.allocator.alloc(u8, new_size);
                        
                        // Copy old data
                        @memcpy(new_data[0..old_size], old_data);
                        
                        // Copy new data
                        @memcpy(new_data[old_size..], data);
                        
                        // Free old data and update pointer
                        self.allocator.free(old_data);
                        col.data_ptr = @ptrCast(new_data.ptr);
                    },
                    else => return error.UnsupportedType,
                }
                
                // Update row count
                self.rows = new_size;
                self.row_count = new_size;
                return;
            }
        }
        
        return error.ColumnNotFound;
    }
    
    pub fn columnRange(self: *Dataset, name: []const u8) !f32 {
        return stats.columnRange(self, name);
    }

    /// Check if a column exists with the given name
    pub fn hasColumn(self: *Dataset, name: []const u8) bool {
        for (self.columns.items) |col| {
            if (std.mem.eql(u8, col.name, name)) {
                return true;
            }
        }
        return false;
    }

    /// Get the type of a column
    pub fn columnType(self: *Dataset, name: []const u8) ?TypeId {
        for (self.columns.items) |col| {
            if (std.mem.eql(u8, col.name, name)) {
                return col.type_id;
            }
        }
        return null;
    }
    
    /// Get all column names as a slice
    /// Note: The caller takes ownership of the returned slice and must free it
    pub fn columnNames(self: *Dataset) [][]const u8 {
        var names = self.allocator.alloc([]const u8, self.columns.items.len) catch return &[_][]const u8{};
        for (self.columns.items, 0..) |col, i| {
            names[i] = col.name;
        }
        return names;
    }
    
    /// Return the number of rows in the dataset
    pub fn len(self: *Dataset) usize {
        return self.rows;
    }
    
    /// Filter the dataset based on a condition
    /// Returns a new dataset containing only rows that match the condition
    pub fn filter(self: *const Dataset, column_name: []const u8, op: filter_mod.ComparisonOp, value: anytype) !Dataset {
        return filter_mod.filter(self, column_name, op, value);
    }
    
    /// Filter the dataset using a custom predicate function
    /// The predicate function takes a row index and the dataset, and returns true if the row should be included
    pub fn filterWithPredicate(self: *const Dataset, predicate: filter_mod.PredicateFn) !Dataset {
        return filter_mod.filterWithPredicate(self, predicate);
    }
    
    /// Get a value at a specific row and column
    /// Useful for custom predicates
    pub fn getColumnValue(self: *const Dataset, row_idx: usize, column_name: []const u8, comptime T: type) !T {
        if (row_idx >= self.rows) return error.InvalidIndex;
        
        const col_idx = self.getColumnIndex(column_name) orelse return error.ColumnNotFound;
        const column = self.columns.items[col_idx];
        
        switch (column.type_id) {
            .int => {
                if (T != i32) return error.TypeMismatch;
                const data = @as([*]i32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .nullable_int => {
                if (T != ?i32) return error.TypeMismatch;
                const data = @as([*]?i32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .float => {
                if (T != f32) return error.TypeMismatch;
                const data = @as([*]f32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .nullable_float => {
                if (T != ?f32) return error.TypeMismatch;
                const data = @as([*]?f32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .string => {
                if (T != []const u8) return error.TypeMismatch;
                const data = @as([*][]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .nullable_string => {
                if (T != ?[]const u8) return error.TypeMismatch;
                const data = @as([*]?[]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .boolean => {
                if (T != bool) return error.TypeMismatch;
                const data = @as([*]bool, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            .nullable_boolean => {
                if (T != ?bool) return error.TypeMismatch;
                const data = @as([*]?bool, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                return data[row_idx];
            },
            else => return error.UnsupportedType,
        }
    }
    
    /// Get the index of a column by name
    pub fn getColumnIndex(self: *const Dataset, name: []const u8) ?usize {
        for (self.columns.items, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) {
                return i;
            }
        }
        return null;
    }
    
    /// Copy a subset of values from a column in this dataset to another dataset
    /// start_idx: The starting index to copy from (inclusive)
    /// end_idx: The ending index to copy to (exclusive)
    pub fn copyColumnTo(self: *Dataset, target_ds: *Dataset, column_name: []const u8, start_idx: usize, end_idx: usize) !void {
        const col_idx = self.getColumnIndex(column_name) orelse return error.ColumnNotFound;
        const column = self.columns.items[col_idx];
        
        // Make sure indices are valid
        if (start_idx >= self.rows) return error.InvalidIndex;
        const safe_end_idx = @min(end_idx, self.rows);
        if (start_idx >= safe_end_idx) return error.InvalidIndex;
        
        const subset_count = safe_end_idx - start_idx;
        
        switch (column.type_id) {
            .int => {
                const data = @as([*]i32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(i32, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .float => {
                const data = @as([*]f32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(f32, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .boolean => {
                const data = @as([*]bool, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(bool, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .char => {
                const data = @as([*]u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(u8, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .string => {
                const data = @as([*][]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc([]const u8, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = try self.allocator.dupe(u8, data[i]);
                }
                try target_ds.addColumn(column.name, subset);
            },
            .nullable_int => {
                const data = @as([*]?i32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(?i32, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .nullable_float => {
                const data = @as([*]?f32, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(?f32, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .nullable_boolean => {
                const data = @as([*]?bool, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(?bool, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .nullable_char => {
                const data = @as([*]?u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(?u8, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    subset[j] = data[i];
                }
                try target_ds.addColumn(column.name, subset);
            },
            .nullable_string => {
                const data = @as([*]?[]const u8, @ptrCast(@alignCast(column.data_ptr)))[0..self.rows];
                var subset = try self.allocator.alloc(?[]const u8, subset_count);
                for (start_idx..safe_end_idx, 0..) |i, j| {
                    if (data[i]) |str| {
                        subset[j] = try self.allocator.dupe(u8, str);
                    } else {
                        subset[j] = null;
                    }
                }
                try target_ds.addColumn(column.name, subset);
            },
            else => return error.UnsupportedType,
        }
    }
};
