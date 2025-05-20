const std = @import("std");

pub const core = @import("core.zig");
pub const operations = @import("operations.zig");
pub const fmt = @import("format.zig");
pub const tests = @import("tests.zig");

/// Re-export the Tensor type for convenience
pub fn Tensor(comptime T: type) type {
    return struct {
        tensor: core.Tensor(T),
        shape: []usize,
        
        pub fn init(allocator: std.mem.Allocator, shape_param: []const usize) !@This() {
            const tensor_obj = try core.Tensor(T).init(allocator, shape_param);
            return .{
                .tensor = tensor_obj,
                .shape = tensor_obj.shape,
            };
        }
        
        pub fn deinit(self: @This()) void {
            self.tensor.deinit();
        }
        
        // Re-export core operations as methods
        pub fn fill(self: *@This(), value: T) void {
            operations.Operations(T).fill(&self.tensor, value);
        }
        
        pub fn size(self: *const @This()) usize {
            return operations.Operations(T).size(&self.tensor);
        }
        
        pub fn rank(self: *const @This()) usize {
            return operations.Operations(T).rank(&self.tensor);
        }
        
        pub fn get(self: *const @This(), indices: []const usize) !T {
            return operations.Operations(T).get(&self.tensor, indices);
        }
        
        pub fn set(self: *@This(), indices: []const usize, value: T) !void {
            return operations.Operations(T).set(&self.tensor, indices, value);
        }
        
        pub fn clone(self: *const @This()) !@This() {
            const cloned_tensor = try operations.Operations(T).clone(&self.tensor);
            return .{
                .tensor = cloned_tensor,
                .shape = cloned_tensor.shape,
            };
        }
        
        pub fn slice(self: *const @This(), start: []const usize, end: []const usize) !@This() {
            const sliced_tensor = try operations.Operations(T).slice(&self.tensor, start, end);
            return .{
                .tensor = sliced_tensor,
                .shape = sliced_tensor.shape,
            };
        }
        
        pub fn reshape(self: *@This(), new_shape: []const usize) !void {
            try operations.Operations(T).reshape(&self.tensor, new_shape);
            self.shape = self.tensor.shape;
        }
        
        pub fn fromColumn(self: *@This(), column: []const T) !@This() {
            const new_tensor = try operations.Operations(T).fromColumn(&self.tensor, column);
            return .{
                .tensor = new_tensor,
                .shape = new_tensor.shape,
            };
        }
        
        // Formatting
        pub fn format(
            self: @This(),
            comptime fmt_str: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            return fmt.Format(T).format(self.tensor, fmt_str, options, writer);
        }
    };
}

test {
    std.testing.refAllDeclsRecursive(@This());
}
