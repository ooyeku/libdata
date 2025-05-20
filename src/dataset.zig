const std = @import("std");

pub const types = @import("dataset/types.zig");
pub const core = @import("dataset/core.zig");
pub const utils = @import("dataset/utils.zig");
pub const format = @import("dataset/format.zig");
pub const csv = @import("dataset/csv.zig");
pub const stats = @import("dataset/stats.zig");
pub const filter = @import("dataset/filter.zig");
pub const stream = @import("dataset/stream.zig");
pub const tests = @import("dataset/tests.zig");

pub const Dataset = core.Dataset;
pub const TypeId = types.TypeId;
pub const NullableValue = types.NullableValue;
pub const DatasetError = types.DatasetError;
pub const ComparisonOp = filter.ComparisonOp;
pub const DatasetStream = stream.DatasetStream;
pub const StreamOptions = stream.StreamOptions;
pub const CSVStreamSource = stream.CSVStreamSource;
pub const DatasetIterator = stream.DatasetIterator;

test {
    std.testing.refAllDeclsRecursive(@This());
}
