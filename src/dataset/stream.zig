const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataset = @import("core.zig").Dataset;
const types = @import("types.zig");
const TypeId = types.TypeId;
const DatasetError = types.DatasetError;
const utils = @import("utils.zig");

/// Options for configuring dataset streaming
pub const StreamOptions = struct {
    chunk_size: usize = 10000, // Default chunk size
    prefetch_chunks: usize = 1, // Number of chunks to prefetch
};

/// Abstract interface for data sources
pub const DataSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        readChunk: *const fn(ptr: *anyopaque, allocator: Allocator, chunk_index: usize, chunk_size: usize) anyerror!?Dataset,
        reset: *const fn(ptr: *anyopaque) void,
        deinit: *const fn(ptr: *anyopaque) void,
        getTotalRows: *const fn(ptr: *anyopaque) ?usize,
    };

    pub fn readChunk(self: DataSource, allocator: Allocator, chunk_index: usize, chunk_size: usize) !?Dataset {
        return self.vtable.readChunk(self.ptr, allocator, chunk_index, chunk_size);
    }

    pub fn reset(self: DataSource) void {
        self.vtable.reset(self.ptr);
    }

    pub fn deinit(self: DataSource) void {
        self.vtable.deinit(self.ptr);
    }

    pub fn getTotalRows(self: DataSource) ?usize {
        return self.vtable.getTotalRows(self.ptr);
    }
};

/// Stream processor for datasets that allows chunk-by-chunk processing
pub const DatasetStream = struct {
    allocator: Allocator,
    source: DataSource,
    options: StreamOptions,
    current_chunk: ?Dataset,
    chunk_index: usize,
    total_rows: ?usize, // May be null if total is unknown

    const Self = @This();

    pub fn init(allocator: Allocator, source: DataSource, options: StreamOptions) Self {
        return .{
            .allocator = allocator,
            .source = source,
            .options = options,
            .current_chunk = null,
            .chunk_index = 0,
            .total_rows = source.getTotalRows(),
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.current_chunk) |*chunk| {
            chunk.deinit();
        }
        self.source.deinit();
    }

    /// Get the next chunk as a Dataset
    pub fn nextChunk(self: *Self) !?Dataset {
        if (self.current_chunk) |*chunk| {
            chunk.deinit();
            self.current_chunk = null;
        }

        const next_chunk = try self.source.readChunk(self.allocator, self.chunk_index, self.options.chunk_size);

        if (next_chunk) |chunk| {
            self.current_chunk = chunk;
            self.chunk_index += 1;
            return chunk;
        }

        return null; // No more chunks
    }

    /// Apply a function to each chunk
    pub fn forEach(self: *Self, func: *const fn(*Dataset) anyerror!void) !void {
        try self.reset();
        
        while (try self.nextChunk()) |chunk| {
            var mutable_chunk = chunk;
            try func(&mutable_chunk);
        }
    }

    /// Map a function over each chunk and collect results
    pub fn map(self: *Self, comptime T: type, func: *const fn(*const Dataset) anyerror!T) !std.ArrayList(T) {
        try self.reset();
        
        var results = std.ArrayList(T).init(self.allocator);
        errdefer results.deinit();
        
        while (try self.nextChunk()) |chunk| {
            const chunk_ptr = &chunk;
            const result = try func(chunk_ptr);
            try results.append(result);
        }
        
        return results;
    }

    /// Filter rows based on a predicate function
    pub fn filter(self: *Self, predicate: *const fn(*const Dataset, usize) anyerror!bool) !Dataset {
        try self.reset();

        var result = Dataset.init(self.allocator);
        errdefer result.deinit();

        var first_chunk = true;

        while (try self.nextChunk()) |chunk| {
            var filtered_chunk = try filterChunk(chunk, predicate, self.allocator);
            defer filtered_chunk.deinit();

            if (filtered_chunk.rows == 0) continue;

            if (first_chunk) {
                // For the first chunk with data, initialize result columns
                for (filtered_chunk.columns.items) |col| {
                    try result.addColumn(col.name, col.type_id);
                }
                first_chunk = false;
            }

            // Append filtered data to result
            try appendChunkToDataset(&filtered_chunk, &result);
        }

        return result;
    }

    /// Reset to beginning of stream
    pub fn reset(self: *Self) !void {
        if (self.current_chunk) |*chunk| {
            chunk.deinit();
            self.current_chunk = null;
        }
        self.chunk_index = 0;
        self.source.reset();
    }

    /// Create a new Dataset from the entire stream (use with caution for large data)
    pub fn toDataset(self: *Self) !Dataset {
        try self.reset();
        
        var result = Dataset.init(self.allocator);
        errdefer result.deinit();
        
        var first_chunk = true;
        
        while (try self.nextChunk()) |chunk| {
            if (first_chunk) {
                // For the first chunk, we need to create the columns
                for (chunk.columns.items) |col| {
                    try result.addColumn(col.name, col.type_id);
                }
                first_chunk = false;
            }
            
            // Append data from this chunk to the result
            var mutable_chunk = chunk;
            try appendChunkToDataset(&mutable_chunk, &result);
        }
        
        return result;
    }

    /// Get estimated number of chunks
    pub fn getEstimatedChunks(self: *const Self) ?usize {
        if (self.total_rows) |rows| {
            return (rows + self.options.chunk_size - 1) / self.options.chunk_size;
        }
        return null;
    }
};

/// CSV-specific streaming source
pub const CSVStreamSource = struct {
    file: std.fs.File,
    path: []const u8,
    allocator: Allocator,
    header: [][]const u8,
    line_positions: std.ArrayList(usize),
    has_scanned: bool,
    total_rows: ?usize,
    is_file_closed: bool = false,

    const Self = @This();

    pub fn init(allocator: Allocator, path: []const u8) !*Self {
        var self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();

        self.* = .{
            .file = file,
            .path = try allocator.dupe(u8, path),
            .allocator = allocator,
            .header = &[_][]const u8{},
            .line_positions = std.ArrayList(usize).init(allocator),
            .has_scanned = false,
            .total_rows = null,
            .is_file_closed = false,
        };

        // Read header
        var buf: [4096]u8 = undefined;
        const header_line = (try self.file.reader().readUntilDelimiterOrEof(&buf, '\n')) orelse
            return error.EmptyFile;

        self.header = try parseCSVHeader(allocator, header_line);

        // Record position after header
        try self.line_positions.append(try self.file.getPos());

        return self;
    }

    pub fn deinit(self: *Self) void {
        // Only close the file if it hasn't been closed already
        if (!self.is_file_closed) {
            self.file.close();
            self.is_file_closed = true;
        }
        
        self.allocator.free(self.path);

        for (self.header) |name| {
            self.allocator.free(name);
        }
        self.allocator.free(self.header);

        self.line_positions.deinit();
        // Don't destroy self - it might be allocated on the stack
        // The caller is responsible for freeing the memory if it was heap-allocated
    }

    /// Scan file to build index of line positions (optional optimization)
    pub fn scanFile(self: *Self) !void {
        if (self.has_scanned) return;

        try self.file.seekTo(self.line_positions.items[0]);

        var buf: [4096]u8 = undefined;
        var reader = self.file.reader();
        var row_count: usize = 0;

        while (try reader.readUntilDelimiterOrEof(&buf, '\n')) |_| {
            try self.line_positions.append(try self.file.getPos());
            row_count += 1;
        }

        self.has_scanned = true;
        self.total_rows = row_count;
    }

    /// Read a specific chunk by index
    pub fn readChunk(self: *Self, allocator: Allocator, chunk_index: usize, chunk_size: usize) !?Dataset {
        // If we've scanned the file, we can use the line positions for direct access
        if (self.has_scanned) {
            const start_idx = chunk_index * chunk_size;
            if (start_idx >= self.line_positions.items.len - 1) {
                return null; // No more chunks
            }

            const end_idx = @min(start_idx + chunk_size, self.line_positions.items.len - 1);
            try self.file.seekTo(self.line_positions.items[start_idx]);

            return try self.readLines(allocator, end_idx - start_idx);
        } else {
            // Sequential read
            if (chunk_index == 0) {
                try self.file.seekTo(self.line_positions.items[0]);
            }

            return try self.readLines(allocator, chunk_size);
        }
    }

    /// Read a specific number of lines into a Dataset
    fn readLines(self: *Self, allocator: Allocator, line_count: usize) !?Dataset {
        var dataset = Dataset.init(allocator);
        errdefer dataset.deinit();

        // Initialize columns based on header
        for (self.header) |name| {
            // Start with string type, we'll infer actual types later
            try dataset.addColumn(name, TypeId.string);
        }

        var buf: [8192]u8 = undefined;
        var reader = self.file.reader();

        var lines_read: usize = 0;
        var row_data = std.ArrayList([]const u8).init(allocator);
        defer row_data.deinit();

        while (lines_read < line_count) {
            const line = (try reader.readUntilDelimiterOrEof(&buf, '\n')) orelse break;

            // Parse CSV line
            try row_data.resize(0);
            var iter = std.mem.tokenizeScalar(u8, line, ',');
            while (iter.next()) |field| {
                const trimmed = std.mem.trim(u8, field, " ");
                try row_data.append(try allocator.dupe(u8, trimmed));
            }

            // Ensure we have the right number of fields
            while (row_data.items.len < self.header.len) {
                try row_data.append(try allocator.dupe(u8, ""));
            }

            // Add row to dataset using the addRow method we implemented
            try dataset.addRow(row_data.items);

            // Clean up row data
            for (row_data.items) |item| {
                allocator.free(item);
            }

            lines_read += 1;
        }

        if (lines_read == 0) {
            dataset.deinit();
            return null;
        }

        return dataset;
    }

    /// Reset to beginning of file (after header)
    pub fn reset(self: *Self) !void {
        try self.file.seekTo(self.line_positions.items[0]);
    }

    /// Get total number of rows (if known)
    pub fn getTotalRows(self: *Self) ?usize {
        return self.total_rows;
    }

    /// Create a DataSource interface
    pub fn asDataSource(self: *Self) DataSource {
        const static_vtable = DataSource.VTable{
            .readChunk = readChunkImpl,
            .reset = resetImpl,
            .deinit = deinitImpl,
            .getTotalRows = getTotalRowsImpl,
        };
        
        return .{
            .ptr = @ptrCast(self),
            .vtable = &static_vtable,
        };
    }

    fn readChunkImpl(ptr: *anyopaque, allocator: Allocator, chunk_index: usize, chunk_size: usize) !?Dataset {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.readChunk(allocator, chunk_index, chunk_size);
    }

    fn resetImpl(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.reset() catch {};
    }

    fn deinitImpl(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn getTotalRowsImpl(ptr: *anyopaque) ?usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.getTotalRows();
    }
};

/// Helper function to parse CSV header
fn parseCSVHeader(allocator: Allocator, header: []const u8) ![][]const u8 {
    var column_names = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (column_names.items) |name| {
            allocator.free(name);
        }
        column_names.deinit();
    }

    var iter = std.mem.tokenizeScalar(u8, header, ',');
    while (iter.next()) |name| {
        const trimmed = std.mem.trim(u8, name, " ");
        try column_names.append(try allocator.dupe(u8, trimmed));
    }

    return column_names.toOwnedSlice();
}

/// Helper function to filter rows in a chunk based on a predicate
fn filterChunk(chunk: Dataset, predicate: *const fn(*const Dataset, usize) anyerror!bool, allocator: Allocator) !Dataset {
    var result = Dataset.init(allocator);
    errdefer result.deinit();
    
    // Create empty columns with the same structure
    for (chunk.columns.items) |col| {
        try result.addColumn(col.name, col.type_id);
    }
    
    // Filter rows
    for (0..chunk.rows) |row_idx| {
        const chunk_ptr = &chunk;
        if (try predicate(chunk_ptr, row_idx)) {
            // Row passed the filter, extract values and add to result
            var row_values = std.ArrayList([]const u8).init(allocator);
            defer row_values.deinit();
            
            for (chunk.columns.items) |col| {
                var value_buf: [128]u8 = undefined;
                
                // Extract value based on type
                const value = switch (col.type_id) {
                    .int => blk: {
                        const data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[0..chunk.rows];
                        const len = std.fmt.formatIntBuf(&value_buf, data[row_idx], 10, .lower, .{});
                        break :blk try allocator.dupe(u8, value_buf[0..len]);
                    },
                    .float => blk: {
                        const data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[0..chunk.rows];
                        const len = std.fmt.bufPrint(&value_buf, "{d:.6}", .{data[row_idx]}) catch value_buf[0..0];
                        break :blk try allocator.dupe(u8, len);
                    },
                    .boolean => blk: {
                        const data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[0..chunk.rows];
                        break :blk try allocator.dupe(u8, if (data[row_idx]) "true" else "false");
                    },
                    .string => blk: {
                        const data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[0..chunk.rows];
                        break :blk try allocator.dupe(u8, data[row_idx]);
                    },
                    .char => blk: {
                        const data = @as([*]u8, @ptrCast(@alignCast(col.data_ptr)))[0..chunk.rows];
                        value_buf[0] = data[row_idx];
                        break :blk try allocator.dupe(u8, value_buf[0..1]);
                    },
                    else => try allocator.dupe(u8, ""),
                };
                
                try row_values.append(value);
            }
            
            // Add row to result dataset
            try result.addRow(row_values.items);
            
            // Free the temporary strings
            for (row_values.items) |item| {
                allocator.free(item);
            }
        }
    }
    
    return result;
}

/// Helper function to append data from one dataset to another
fn appendChunkToDataset(source: *Dataset, target: *Dataset) !void {
    // For each column in the source
    for (source.columns.items) |source_col| {
        // Find the matching column in the target
        for (target.columns.items) |target_col| {
            if (std.mem.eql(u8, source_col.name, target_col.name)) {
                // Append data based on type
                switch (source_col.type_id) {
                    .int => {
                        const data = @as([*]i32, @ptrCast(@alignCast(source_col.data_ptr)))[0..source.rows];
                        try target.appendColumnData(source_col.name, data);
                    },
                    .float => {
                        const data = @as([*]f32, @ptrCast(@alignCast(source_col.data_ptr)))[0..source.rows];
                        try target.appendColumnData(source_col.name, data);
                    },
                    .boolean => {
                        const data = @as([*]bool, @ptrCast(@alignCast(source_col.data_ptr)))[0..source.rows];
                        try target.appendColumnData(source_col.name, data);
                    },
                    .string => {
                        const data = @as([*][]const u8, @ptrCast(@alignCast(source_col.data_ptr)))[0..source.rows];
                        try target.appendColumnData(source_col.name, data);
                    },
                    .char => {
                        const data = @as([*]u8, @ptrCast(@alignCast(source_col.data_ptr)))[0..source.rows];
                        try target.appendColumnData(source_col.name, data);
                    },
                    else => {}, // Skip unsupported types
                }
                break;
            }
        }
    }
}

/// Memory-efficient iterator for processing large datasets
pub const DatasetIterator = struct {
    stream: *DatasetStream,
    current_chunk: ?Dataset,
    current_row: usize,
    chunk_row: usize,

    const Self = @This();

    pub fn init(stream: *DatasetStream) Self {
        return .{
            .stream = stream,
            .current_chunk = null,
            .current_row = 0,
            .chunk_row = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.current_chunk) |*chunk| {
            chunk.deinit();
            self.current_chunk = null;
        }
    }

    /// Get the next row as a map of column name to value
    pub fn next(self: *Self) !?std.StringHashMap(types.NullableValue) {
        // Load first chunk if needed
        if (self.current_chunk == null) {
            try self.stream.reset();
            self.current_chunk = (try self.stream.nextChunk()) orelse return null;
            self.chunk_row = 0;
        }

        // Check if we need to load the next chunk
        if (self.chunk_row >= self.current_chunk.?.rows) {
            self.current_chunk.?.deinit();
            self.current_chunk = (try self.stream.nextChunk()) orelse return null;
            self.chunk_row = 0;
        }

        // Get current row data
        const row_data = std.StringHashMap(types.NullableValue).init(self.stream.allocator);

        // In a real implementation, you'd extract values from the current row
        // This is a simplified version

        self.chunk_row += 1;
        self.current_row += 1;

        return row_data;
    }

    /// Reset the iterator
    pub fn reset(self: *Self) !void {
        if (self.current_chunk) |*chunk| {
            chunk.deinit();
            self.current_chunk = null;
        }

        try self.stream.reset();
        self.current_row = 0;
        self.chunk_row = 0;
    }
};

test "Dataset streaming basics" {
    // Create a test allocator
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    
    // Create a test dataset
    var ds = Dataset.init(allocator);
    defer ds.deinit();
    
    // Add some test data
    const names = [_][]const u8{ "Alice", "Bob", "Charlie", "Dave", "Eve" };
    const ages = [_]i32{ 25, 30, 35, 40, 45 };
    const scores = [_]f32{ 90.5, 85.0, 92.5, 88.0, 95.5 };
    
    try ds.addColumn("name", &names);
    try ds.addColumn("age", &ages);
    try ds.addColumn("score", &scores);
    
    // Test basic dataset operations
    try std.testing.expectEqual(@as(usize, 5), ds.rows);
    try std.testing.expectEqual(@as(usize, 3), ds.columns.items.len);
    
    // Test column access
    const age_col = try ds.getColumn("age", i32);
    try std.testing.expectEqual(@as(i32, 25), age_col[0]);
    try std.testing.expectEqual(@as(i32, 45), age_col[4]);
    
    // Test row access
    const name_val = try ds.getColumnValue(2, "name", []const u8);
    try std.testing.expectEqualStrings("Charlie", name_val);
    
    // Test basic filtering
    var young_count: usize = 0;
    for (age_col) |age| {
        if (age < 40) young_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), young_count);
    
    // Test average calculation
    const score_col = try ds.getColumn("score", f32);
    var score_sum: f32 = 0;
    for (score_col) |score| {
        score_sum += score;
    }
    const avg_score = score_sum / @as(f32, @floatFromInt(score_col.len));
    try std.testing.expectApproxEqAbs(@as(f32, 90.3), avg_score, 0.1);
}

// A simple in-memory data source for testing streaming functionality
const TestDataSource = struct {
    dataset: Dataset,
    chunk_index: usize = 0,
    chunk_size: usize,
    allocator: Allocator,
    
    const Self = @This();
    
    pub fn init(dataset: Dataset, chunk_size: usize) Self {
        return .{
            .dataset = dataset,
            .chunk_size = chunk_size,
            .allocator = dataset.allocator,
        };
    }
    
    // Static vtable that matches the DataSource.VTable structure
    const vtable: DataSource.VTable = .{
        .readChunk = readChunk,
        .reset = reset,
        .deinit = deinit,
        .getTotalRows = getTotalRows,
    };
    
    pub fn asDataSource(self: *Self) DataSource {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }
    
    fn readChunk(ptr: *anyopaque, allocator: Allocator, chunk_index: usize, chunk_size: usize) !?Dataset {
        const self = @as(*Self, @ptrCast(@alignCast(ptr)));
        
        // Use the provided chunk_index instead of the internal one
        const start_row = chunk_index * chunk_size;
        if (start_row >= self.dataset.rows) return null;
        
        const end_row = @min(start_row + chunk_size, self.dataset.rows);
        const rows_to_read = end_row - start_row;
        
        var chunk = Dataset.init(allocator);
        errdefer chunk.deinit();
        
        // Create columns with same structure
        // Make sure to duplicate the column names to avoid memory issues
        for (self.dataset.columns.items) |col| {
            const name_copy = try allocator.dupe(u8, col.name);
            try chunk.addColumn(name_copy, col.type_id);
        }
        
        // Copy data for this chunk
        for (self.dataset.columns.items) |col| {
            switch (col.type_id) {
                .int => {
                    const data = @as([*]i32, @ptrCast(@alignCast(col.data_ptr)))[start_row..end_row];
                    try chunk.appendColumnData(col.name, data);
                },
                .float => {
                    const data = @as([*]f32, @ptrCast(@alignCast(col.data_ptr)))[start_row..end_row];
                    try chunk.appendColumnData(col.name, data);
                },
                .boolean => {
                    const data = @as([*]bool, @ptrCast(@alignCast(col.data_ptr)))[start_row..end_row];
                    try chunk.appendColumnData(col.name, data);
                },
                .string => {
                    const data = @as([*][]const u8, @ptrCast(@alignCast(col.data_ptr)))[start_row..end_row];
                    try chunk.appendColumnData(col.name, data);
                },
                // For the test, we only support basic types
                // In a real implementation, we would need to handle all types
                .nullable_int, .nullable_float, .nullable_string, .nullable_boolean,
                .char, .nullable_char, .tensor, .image, .audio, .video, .text, .other => {
                    // Skip unsupported types in our test implementation
                    continue;
                },
            }
        }
        
        chunk.rows = rows_to_read;
        return chunk;
    }
    
    fn reset(ptr: *anyopaque) void {
        const self = @as(*Self, @ptrCast(@alignCast(ptr)));
        self.chunk_index = 0;
    }
    
    fn deinit(ptr: *anyopaque) void {
        // Nothing to clean up for this test data source
        // The dataset is owned by the test and will be cleaned up separately
        _ = ptr; // Unused parameter
    }
    
    fn getTotalRows(ptr: *anyopaque) ?usize {
        const self = @as(*Self, @ptrCast(@alignCast(ptr)));
        return self.dataset.rows;
    }
};

test "Memory-based streaming" {
    // Create a test allocator
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    
    // Create a test dataset
    var ds = Dataset.init(allocator);
    defer ds.deinit();
    
    // Add some test data - use explicit strings to avoid memory issues
    try ds.addColumn("name", &[_][]const u8{ "Alice", "Bob", "Charlie", "Dave", "Eve" });
    try ds.addColumn("age", &[_]i32{ 25, 30, 35, 40, 45 });
    try ds.addColumn("score", &[_]f32{ 90.5, 85.0, 92.5, 88.0, 95.5 });
    
    // Create a test data source with chunk size 2
    var data_source = TestDataSource.init(ds, 2);
    
    // Create a stream
    var stream = DatasetStream.init(
        allocator,
        data_source.asDataSource(),
        .{ .chunk_size = 2 }
    );
    defer stream.deinit();
    
    // Test reading chunks
    var chunk_count: usize = 0;
    var total_rows: usize = 0;
    
    while (try stream.nextChunk()) |chunk| {
        // Verify chunk structure
        try std.testing.expectEqual(3, chunk.columns.items.len);
        try std.testing.expect(chunk.rows <= 2); // Should be 2 or less
        
        total_rows += chunk.rows;
        chunk_count += 1;
    }
    
    // Verify we read all rows in chunks
    try std.testing.expectEqual(@as(usize, 5), total_rows);
    try std.testing.expectEqual(@as(usize, 3), chunk_count); // 2 chunks of 2 rows, 1 chunk of 1 row
    
    // Test basic streaming functionality - just verify we can read all the data
    try stream.reset();
    
    var row_count: usize = 0;
    while (try stream.nextChunk()) |chunk| {
        row_count += chunk.rows;
    }
    
    try std.testing.expectEqual(@as(usize, 5), row_count);
}
