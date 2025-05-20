const std = @import("std");
const core = @import("core.zig");

/// Basic tensor operations
pub fn Operations(comptime T: type) type {
    const TensorT = core.Tensor(T);
    
    return struct {
        /// Fill the entire tensor with a value
        pub fn fill(tensor: *TensorT, value: T) void {
            @memset(tensor.data, value);
        }

        /// Get the total number of elements in the tensor
        pub fn size(tensor: *const TensorT) usize {
            var total: usize = 1;
            for (tensor.shape) |dim| {
                total *= dim;
            }
            return total;
        }

        /// Get the number of dimensions
        pub fn rank(tensor: *const TensorT) usize {
            return tensor.shape.len;
        }

        /// Get value at indices
        pub fn get(tensor: *const TensorT, indices: []const usize) !T {
            const idx = try tensor.calculateIndex(indices);
            return tensor.data[idx];
        }

        /// Set value at indices
        pub fn set(tensor: *TensorT, indices: []const usize, value: T) !void {
            const idx = try tensor.calculateIndex(indices);
            tensor.data[idx] = value;
        }

        /// Create a copy of the tensor
        pub fn clone(tensor: *const TensorT) !TensorT {
            const new_tensor = try TensorT.init(tensor.allocator, tensor.shape);
            @memcpy(new_tensor.data, tensor.data);
            return new_tensor;
        }

        /// Create a view/slice of the tensor (without copying data)
        pub fn slice(tensor: *const TensorT, start: []const usize, end: []const usize) !TensorT {
            if (start.len != tensor.shape.len or end.len != tensor.shape.len) {
                return error.DimensionMismatch;
            }

            // Calculate new shape and validate bounds
            const new_shape = try tensor.allocator.alloc(usize, tensor.shape.len);
            for (start, end, tensor.shape, 0..) |s, e, dim, i| {
                if (s >= dim or e > dim or s >= e) {
                    return error.IndexOutOfBounds;
                }
                new_shape[i] = e - s;
            }

            // Calculate new offset
            var new_offset = tensor.offset;
            for (start, 0..) |s, i| {
                new_offset += s * tensor.strides[i];
            }

            // Create new tensor sharing the same data
            return TensorT{
                .data = tensor.data,
                .shape = new_shape,
                .strides = try tensor.allocator.dupe(usize, tensor.strides),
                .offset = new_offset,
                .allocator = tensor.allocator,
            };
        }

        /// Reshape tensor to new dimensions (if possible)
        pub fn reshape(tensor: *TensorT, new_shape: []const usize) !void {
            // Calculate new total size
            var new_size: usize = 1;
            for (new_shape) |dim| {
                new_size *= dim;
            }

            // Verify size matches
            if (new_size != size(tensor)) {
                return error.InvalidShape;
            }

            // Update shape and strides
            const shape_copy = try tensor.allocator.alloc(usize, new_shape.len);
            @memcpy(shape_copy, new_shape);

            const strides = try tensor.allocator.alloc(usize, new_shape.len);
            var stride: usize = 1;
            var i: usize = new_shape.len;
            while (i > 0) {
                i -= 1;
                strides[i] = stride;
                stride *= new_shape[i];
            }

            // Free old arrays and update
            tensor.allocator.free(tensor.shape);
            tensor.allocator.free(tensor.strides);
            tensor.shape = shape_copy;
            tensor.strides = strides;
        }

        pub fn fromColumn(tensor: *TensorT, column: []const T) !TensorT {
            return TensorT.init(tensor.allocator, column);
        }
    };
}
