const std = @import("std");

// Re-export the tensor module
pub const core = @import("tensor/core.zig");
pub const operations = @import("tensor/operations.zig");
pub const fmt = @import("tensor/format.zig");
pub const tests = @import("tensor/tests.zig");

/// Re-export the Tensor type for convenience
pub fn Tensor(comptime T: type) type {
    return @import("tensor/api.zig").Tensor(T);
}

test {
    std.testing.refAllDeclsRecursive(@This());
}
