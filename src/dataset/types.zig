const std = @import("std");

// Type definitions
pub const TypeId = enum {
    int,
    nullable_int,
    float,
    nullable_float,
    string,
    nullable_string,
    char,
    nullable_char,
    boolean,
    nullable_boolean,
    tensor,
    image,
    audio,
    video,
    text,
    other,
};

pub const NullableValue = union(enum) {
    int: ?i32,
    float: ?f32,
    string: ?[]const u8,
    char: ?u8,
    boolean: ?bool,
};

pub const ColumnMeta = struct {
    name: []const u8,
    data_ptr: *anyopaque,
    type_id: TypeId,
};

pub const DatasetError = error{
    ColumnNotFound,
    LengthMismatch,
    InvalidType,
    EmptyFile,
    InvalidFormat,
    InvalidIndex,
    UnsupportedType,
};
