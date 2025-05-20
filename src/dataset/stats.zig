const std = @import("std");
const Dataset = @import("core.zig").Dataset;

/// Calculate the minimum value in a column
pub fn columnMin(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    var min: f32 = @floatFromInt(data[0]);
    for (data) |value| {
        const float_val: f32 = @floatFromInt(value);
        if (float_val < min) {
            min = float_val;
        }
    }
    return min;
}

/// Calculate the maximum value in a column
pub fn columnMax(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    var max: f32 = @floatFromInt(data[0]);
    for (data) |value| {
        const float_val: f32 = @floatFromInt(value);
        if (float_val > max) {
            max = float_val;
        }
    }
    return max;
}

/// Calculate the mean value of a column
pub fn columnMean(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    var sum: f32 = 0;
    for (data) |value| {
        sum += @floatFromInt(value);
    }
    return sum / @as(f32, @floatFromInt(dataset.rows));
}

/// Calculate the standard deviation of a column
pub fn columnStdDev(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    const mean = try columnMean(dataset, name);
    var sum: f32 = 0;
    for (data) |value| {
        const float_val: f32 = @floatFromInt(value);
        const diff = float_val - mean;
        sum += diff * diff;
    }
    return @sqrt(sum / @as(f32, @floatFromInt(dataset.rows)));
}

/// Calculate the median value of a column
pub fn columnMedian(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    const data_copy = try dataset.allocator.dupe(i32, data);
    defer dataset.allocator.free(data_copy);

    std.mem.sort(i32, data_copy[0..], {}, std.sort.asc(i32));
    const mid = dataset.rows / 2;
    return @floatFromInt(data_copy[mid]);
}

/// Calculate the mode (most frequent value) of a column
pub fn columnMode(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    if (data.len == 0) return 0;

    // Create a map to count occurrences
    var counts = std.AutoHashMap(i32, usize).init(dataset.allocator);
    defer counts.deinit();

    // Count occurrences of each value
    for (data) |value| {
        const entry = try counts.getOrPut(value);
        if (entry.found_existing) {
            entry.value_ptr.* += 1;
        } else {
            entry.value_ptr.* = 1;
        }
    }

    // Find the most frequent value
    var mode_value: i32 = data[0];
    var max_count: usize = 0;

    var it = counts.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* > max_count) {
            max_count = entry.value_ptr.*;
            mode_value = entry.key_ptr.*;
        }
    }

    return @floatFromInt(mode_value);
}

/// Calculate the range (max - min) of a column
pub fn columnRange(dataset: *Dataset, name: []const u8) !f32 {
    const min = try columnMin(dataset, name);
    const max = try columnMax(dataset, name);
    return max - min;
}

/// Calculate the variance of a column
pub fn columnVariance(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    const mean = try columnMean(dataset, name);
    var sum: f32 = 0;
    
    for (data) |value| {
        const float_val: f32 = @floatFromInt(value);
        const diff = float_val - mean;
        sum += diff * diff;
    }
    
    return sum / @as(f32, @floatFromInt(dataset.rows));
}

/// Calculate a specific percentile of a column
pub fn columnPercentile(dataset: *Dataset, name: []const u8, percentile: f32) !f32 {
    if (percentile < 0 or percentile > 100) {
        return error.InvalidPercentile;
    }
    
    const data = try dataset.getColumn(name, i32);
    const data_copy = try dataset.allocator.dupe(i32, data);
    defer dataset.allocator.free(data_copy);

    std.mem.sort(i32, data_copy, {}, std.sort.asc(i32));
    
    const index = @as(usize, @intFromFloat(@as(f32, @floatFromInt(data_copy.len - 1)) * percentile / 100.0));
    return @floatFromInt(data_copy[index]);
}

/// Calculate the first quartile (25th percentile) of a column
pub fn columnQ1(dataset: *Dataset, name: []const u8) !f32 {
    return columnPercentile(dataset, name, 25);
}

/// Calculate the third quartile (75th percentile) of a column
pub fn columnQ3(dataset: *Dataset, name: []const u8) !f32 {
    return columnPercentile(dataset, name, 75);
}

/// Calculate the interquartile range (IQR) of a column
pub fn columnIQR(dataset: *Dataset, name: []const u8) !f32 {
    const q1 = try columnQ1(dataset, name);
    const q3 = try columnQ3(dataset, name);
    return q3 - q1;
}

/// Detect outliers in a column using the IQR method
/// Returns a boolean array where true indicates an outlier
pub fn columnOutliers(dataset: *Dataset, name: []const u8) ![]bool {
    const data = try dataset.getColumn(name, i32);
    const q1 = try columnQ1(dataset, name);
    const q3 = try columnQ3(dataset, name);
    const iqr = q3 - q1;
    
    const lower_bound = q1 - 1.5 * iqr;
    const upper_bound = q3 + 1.5 * iqr;
    
    var outliers = try dataset.allocator.alloc(bool, data.len);
    
    for (data, 0..) |value, i| {
        const float_val: f32 = @floatFromInt(value);
        outliers[i] = float_val < lower_bound or float_val > upper_bound;
    }
    
    return outliers;
}

/// Calculate the skewness of a column (measure of asymmetry)
pub fn columnSkewness(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    const mean = try columnMean(dataset, name);
    const std_dev = try columnStdDev(dataset, name);
    
    if (std_dev == 0) return 0; // No variation
    
    var sum: f32 = 0;
    for (data) |value| {
        const float_val: f32 = @floatFromInt(value);
        const z = (float_val - mean) / std_dev;
        sum += z * z * z;
    }
    
    return sum / @as(f32, @floatFromInt(data.len));
}

/// Calculate the kurtosis of a column (measure of "tailedness")
pub fn columnKurtosis(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    const mean = try columnMean(dataset, name);
    const std_dev = try columnStdDev(dataset, name);
    
    if (std_dev == 0) return 0; // No variation
    
    var sum: f32 = 0;
    for (data) |value| {
        const float_val: f32 = @floatFromInt(value);
        const z = (float_val - mean) / std_dev;
        sum += z * z * z * z;
    }
    
    return sum / @as(f32, @floatFromInt(data.len)) - 3.0; // Excess kurtosis (normal = 0)
}

/// Calculate the covariance between two columns
pub fn columnCovariance(dataset: *Dataset, name1: []const u8, name2: []const u8) !f32 {
    const data1 = try dataset.getColumn(name1, i32);
    const data2 = try dataset.getColumn(name2, i32);
    
    if (data1.len != data2.len) {
        return error.LengthMismatch;
    }
    
    const mean1 = try columnMean(dataset, name1);
    const mean2 = try columnMean(dataset, name2);
    
    var sum: f32 = 0;
    for (data1, data2) |x, y| {
        const x_float: f32 = @floatFromInt(x);
        const y_float: f32 = @floatFromInt(y);
        sum += (x_float - mean1) * (y_float - mean2);
    }
    
    return sum / @as(f32, @floatFromInt(data1.len));
}

/// Calculate the Pearson correlation coefficient between two columns
pub fn columnCorrelation(dataset: *Dataset, name1: []const u8, name2: []const u8) !f32 {
    const data1 = try dataset.getColumn(name1, i32);
    const data2 = try dataset.getColumn(name2, i32);
    
    if (data1.len != data2.len) {
        return error.LengthMismatch;
    }
    
    const mean1 = try columnMean(dataset, name1);
    const mean2 = try columnMean(dataset, name2);
    
    var sum_xy: f32 = 0;
    var sum_x2: f32 = 0;
    var sum_y2: f32 = 0;
    
    for (data1, data2) |x, y| {
        const x_float: f32 = @floatFromInt(x);
        const y_float: f32 = @floatFromInt(y);
        
        const diff_x = x_float - mean1;
        const diff_y = y_float - mean2;
        
        sum_xy += diff_x * diff_y;
        sum_x2 += diff_x * diff_x;
        sum_y2 += diff_y * diff_y;
    }
    
    if (sum_x2 == 0 or sum_y2 == 0) {
        return 0; // No variation in at least one column
    }
    
    return sum_xy / (@sqrt(sum_x2) * @sqrt(sum_y2));
}

/// Calculate the sum of a column
pub fn columnSum(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, i32);
    var sum: f32 = 0;
    for (data) |value| {
        sum += @floatFromInt(value);
    }
    return sum;
}

/// Calculate the count of non-null values in a column
pub fn columnCount(dataset: *Dataset, name: []const u8) !usize {
    // For nullable columns
    if (dataset.columnType(name) == .nullable_int) {
        const data = try dataset.getColumn(name, ?i32);
        var count: usize = 0;
        for (data) |value| {
            if (value != null) count += 1;
        }
        return count;
    } else {
        // For non-nullable columns, just return the row count
        return dataset.rows;
    }
}

/// Calculate the frequency distribution of a column
/// Returns a hashmap of values to their frequencies
pub fn columnFrequency(dataset: *Dataset, name: []const u8) !std.AutoHashMap(i32, usize) {
    const data = try dataset.getColumn(name, i32);
    var freq = std.AutoHashMap(i32, usize).init(dataset.allocator);
    
    for (data) |value| {
        const entry = try freq.getOrPut(value);
        if (entry.found_existing) {
            entry.value_ptr.* += 1;
        } else {
            entry.value_ptr.* = 1;
        }
    }
    
    return freq;
}

/// Calculate the mean of a float column
pub fn columnMeanFloat(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, f32);
    var sum: f32 = 0;
    for (data) |value| {
        sum += value;
    }
    return sum / @as(f32, @floatFromInt(data.len));
}

/// Calculate the mean of a nullable integer column
pub fn columnMeanNullable(dataset: *Dataset, name: []const u8) !f32 {
    const data = try dataset.getColumn(name, ?i32);
    var sum: f32 = 0;
    var count: usize = 0;
    
    for (data) |maybe_value| {
        if (maybe_value) |value| {
            sum += @floatFromInt(value);
            count += 1;
        }
    }
    
    if (count == 0) return 0;
    return sum / @as(f32, @floatFromInt(count));
}
