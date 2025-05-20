const std = @import("std");
const Allocator = std.mem.Allocator;

/// Generic tensor implementation that can hold any numeric type
pub fn Tensor(comptime T: type) type {
    return struct {
        data: []T,
        shape: []usize,
        strides: []usize,
        offset: usize,
        allocator: Allocator,

        pub fn init(allocator: Allocator, shape: []const usize) !@This() {
            // Calculate total size by multiplying all dimensions
            var total_size: usize = 1;
            for (shape) |dim| {
                total_size *= dim;
            }
            const data = try allocator.alloc(T, total_size);

            // Calculate strides (row-major order)
            const strides = try allocator.alloc(usize, shape.len);
            var stride: usize = 1;
            var i: usize = shape.len;
            while (i > 0) {
                i -= 1;
                strides[i] = stride;
                stride *= shape[i];
            }

            // Allocate the tensor
            const shape_copy = try allocator.alloc(usize, shape.len);
            @memcpy(shape_copy, shape);

            return @This(){
                .data = data,
                .shape = shape_copy,
                .strides = strides,
                .offset = 0,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: @This()) void {
            self.allocator.free(self.data);
            self.allocator.free(self.shape);
            self.allocator.free(self.strides);
        }

        /// Calculate flat index from dimensional indices
        pub fn calculateIndex(self: *const @This(), indices: []const usize) !usize {
            if (indices.len != self.shape.len) {
                return error.DimensionMismatch;
            }

            var index = self.offset;
            for (indices, 0..) |idx, dim| {
                if (idx >= self.shape[dim]) {
                    return error.IndexOutOfBounds;
                }
                index += idx * self.strides[dim];
            }
            return index;
        }
    };
}
