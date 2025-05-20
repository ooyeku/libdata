const std = @import("std");
const testing = std.testing;
const dataset = @import("dataset.zig");
const tensor = @import("tensor.zig");

pub const Dataset = dataset.Dataset;
pub const Tensor = tensor.Tensor;

// Re-export dataset types for convenience
pub const TypeId = dataset.TypeId;
pub const DatasetError = dataset.DatasetError;
pub const ComparisonOp = dataset.ComparisonOp;
pub const filter = dataset.filter;

// Export streaming functionality
pub const DatasetStream = dataset.DatasetStream;
pub const StreamOptions = dataset.StreamOptions;
pub const CSVStreamSource = dataset.CSVStreamSource;
pub const DatasetIterator = dataset.DatasetIterator;
pub fn setup() void {
    std.debug.print("Welcome to LibML!\n", .{});
}

test {
    setup();
    _ = Dataset;
    _ = Tensor;
}
