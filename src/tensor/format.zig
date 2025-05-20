const std = @import("std");
const core = @import("core.zig");

/// Formatting functions for tensor display
pub fn Format(comptime T: type) type {
    const TensorT = core.Tensor(T);
    
    return struct {
        /// Print the tensor (useful for debugging)
        pub fn format(
            tensor: TensorT,
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;

            try writer.print("Tensor(shape={any}, data=[", .{tensor.shape});
            for (tensor.data, 0..) |value, i| {
                if (i > 0) try writer.writeAll(", ");
                try writer.print("{d}", .{value});
            }
            try writer.writeAll("])");
        }
    };
}
