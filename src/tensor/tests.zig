const std = @import("std");
const testing = std.testing;
const core = @import("core.zig");
const ops = @import("operations.zig");

test "tensor init and deinit" {
    const allocator = testing.allocator;
    const shape = [_]usize{ 3, 4 };
    const tensor = try core.Tensor(f32).init(allocator, &shape);
    defer tensor.deinit();
}

test "tensor operations" {
    const allocator = testing.allocator;
    const shape = [_]usize{ 2, 3 };
    var tensor = try core.Tensor(f32).init(allocator, &shape);
    defer tensor.deinit();

    const TensorOps = ops.Operations(f32);

    // Test fill
    TensorOps.fill(&tensor, 42);
    try testing.expectEqual(@as(f32, 42), try TensorOps.get(&tensor, &[_]usize{ 0, 0 }));

    // Test size and rank
    try testing.expectEqual(@as(usize, 6), TensorOps.size(&tensor));
    try testing.expectEqual(@as(usize, 2), TensorOps.rank(&tensor));

    // Test set/get
    try TensorOps.set(&tensor, &[_]usize{ 1, 2 }, 123);
    try testing.expectEqual(@as(f32, 123), try TensorOps.get(&tensor, &[_]usize{ 1, 2 }));

    // Test clone
    var clone = try TensorOps.clone(&tensor);
    defer clone.deinit();
    try testing.expectEqual(@as(f32, 123), try TensorOps.get(&clone, &[_]usize{ 1, 2 }));

    // Test reshape
    try TensorOps.reshape(&tensor, &[_]usize{ 3, 2 });
    try testing.expectEqual(@as(usize, 2), TensorOps.rank(&tensor));
    try testing.expectEqual(@as(usize, 6), TensorOps.size(&tensor));
}
