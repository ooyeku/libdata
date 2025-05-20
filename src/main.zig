const std = @import("std");
const libdata = @import("root.zig");
const Dataset = libdata.Dataset;
const Tensor = libdata.Tensor(f32);

/// Demonstrates basic tensor operations
fn demonstrateTensor(allocator: std.mem.Allocator, writer: anytype) !void {
    try writer.print("\n==== Tensor Demonstration ====\n\n", .{});
    
    // 1. Create a 2D tensor (matrix) with shape [3,4]
    try writer.print("Creating a 30x40 tensor...\n", .{});
    const shape2d = [_]usize{30, 40};
    var tensor2d = try Tensor.init(allocator, &shape2d);
    defer tensor2d.deinit();
    
    // 2. Fill the tensor with initial values
    tensor2d.fill(0.0);
    try writer.print("Initialized tensor: {any}\n", .{tensor2d});
    
    // 3. Set specific values
    try writer.print("\nSetting specific values...\n", .{});
    try tensor2d.set(&[_]usize{0, 0}, 1.0);
    try tensor2d.set(&[_]usize{0, 1}, 2.0);
    try tensor2d.set(&[_]usize{1, 0}, 3.0);
    try tensor2d.set(&[_]usize{1, 1}, 4.0);
    try tensor2d.set(&[_]usize{2, 2}, 5.0);
    try tensor2d.set(&[_]usize{2, 3}, 6.0);
    
    // 4. Visualize the tensor as a matrix
    try writer.print("\nVisualizing 30x40 tensor as matrix:\n", .{});
    for (0..shape2d[0]) |i| {
        try writer.writeAll("[");
        for (0..shape2d[1]) |j| {
            const val = try tensor2d.get(&[_]usize{i, j});
            if (j > 0) try writer.writeAll(", ");
            try writer.print("{d:.1}", .{val});
        }
        try writer.print("]\n", .{});
    }
    
    // 5. Create a clone of the tensor
    try writer.print("\nCloning the tensor...\n", .{});
    var tensor_clone = try tensor2d.clone();
    defer tensor_clone.deinit();
    try writer.print("Original tensor: {any}\n", .{tensor2d});
    try writer.print("Cloned tensor: {any}\n", .{tensor_clone});
    
    // 6. Modify the clone to show they're separate
    try writer.print("\nModifying only the clone...\n", .{});
    try tensor_clone.set(&[_]usize{0, 0}, 99.0);
    try writer.print("Original at [0,0]: {d:.1}\n", .{try tensor2d.get(&[_]usize{0, 0})});
    try writer.print("Clone at [0,0]: {d:.1}\n", .{try tensor_clone.get(&[_]usize{0, 0})});
    
    // 7. Reshape the tensor
    try writer.print("\nReshaping 30x40 tensor to 40x30...\n", .{});
    try tensor2d.reshape(&[_]usize{40, 30});
    try writer.print("Reshaped tensor: {any}\n", .{tensor2d});
    try writer.print("New shape: {d}x{d}\n", .{tensor2d.shape[0], tensor2d.shape[1]});
    
    // 8. Visualize the reshaped tensor
    try writer.print("\nVisualizing reshaped 40x30 tensor:\n", .{});
    for (0..tensor2d.shape[0]) |i| {
        try writer.writeAll("[");
        for (0..tensor2d.shape[1]) |j| {
            const val = try tensor2d.get(&[_]usize{i, j});
            if (j > 0) try writer.writeAll(", ");
            try writer.print("{d:.1}", .{val});
        }
        try writer.print("]\n", .{});
    }
    
    // 9. Create a 3D tensor
    try writer.print("\nCreating a 3D tensor of shape [2,2,3]...\n", .{});
    const shape3d = [_]usize{2, 2, 3};
    var tensor3d = try Tensor.init(allocator, &shape3d);
    defer tensor3d.deinit();
    
    // 10. Fill with values
    tensor3d.fill(1.0);
    var counter: f32 = 1.0;
    for (0..shape3d[0]) |i| {
        for (0..shape3d[1]) |j| {
            for (0..shape3d[2]) |k| {
                try tensor3d.set(&[_]usize{i, j, k}, counter);
                counter += 1.0;
            }
        }
    }
    
    // 11. Visualize 3D tensor
    try writer.print("\nVisualizing 3D tensor (2,2,3):\n", .{});
    for (0..shape3d[0]) |i| {
        try writer.print("Slice i={d}:\n", .{i});
        for (0..shape3d[1]) |j| {
            try writer.writeAll("  [");
            for (0..shape3d[2]) |k| {
                const val = try tensor3d.get(&[_]usize{i, j, k});
                if (k > 0) try writer.writeAll(", ");
                try writer.print("{d:.1}", .{val});
            }
            try writer.print("]\n", .{});
        }
    }
    
    try writer.print("\n==== End of Tensor Demonstration ====\n\n", .{});
}

pub fn main() !void {
    libdata.setup();

    const allocator = std.heap.page_allocator;
    var ds = Dataset.init(allocator);
    defer ds.deinit();

    const names = [_][]const u8{ "Alice", "Bob", "Charlie" };
    const ages = [_]u32{ 25, 30, 35 };
    const heights = [_]f32{ 1.75, 1.80, 1.85 };
    const weights = [_]f32{ 55.0, 65.0, 75.0 };
    const genders = [_]u8{ 'F', 'M', 'M' };
    const colors = [_]u8{ 'R', 'G', 'B' };
    const is_student = [_]bool{ true, false, true };
    const is_employed = [_]?bool{ true, null, false };

    try ds.addColumn("name", &names);
    try ds.addColumn("age", &ages);
    try ds.addColumn("height", &heights);
    try ds.addColumn("weight", &weights);
    try ds.addColumn("gender", &genders);
    try ds.addColumn("color", &colors);
    try ds.addColumn("is_student", &is_student);
    try ds.addColumn("is_employed", &is_employed);
    const stdout = std.io.getStdOut().writer();
    try ds.formatAsTable(stdout);
    try ds.format(" ", .{}, stdout);

    // Create a stats dataset
    var stats = Dataset.init(allocator);
    defer stats.deinit();

    const stat_names = [_][]const u8{ "Min", "Max", "Range", "Mean", "StdDev", "Median", "Mode" };
    const stat_values = [_]f32{
        try ds.columnMin("age"),
        try ds.columnMax("age"),
        try ds.columnRange("age"),
        try ds.columnMean("age"),
        try ds.columnStdDev("age"),
        try ds.columnMedian("age"),
        try ds.columnMode("age"),
    };

    try stats.addColumn("Statistic", &stat_names);
    try stats.addColumn("Value", &stat_values);

    try stdout.print("\nAge Statistics:\n", .{});
    try stats.formatAsTable(stdout);

    // Export datasets to CSV
    try ds.toCSV("demo.csv");
    try stats.toCSV("stats.csv");
    defer std.fs.cwd().deleteFile("demo.csv") catch {};
    defer std.fs.cwd().deleteFile("stats.csv") catch {};
    
    // Test mode calculation with different datasets
    try stdout.print("\nOriginal dataset - Age Mode: {d:.2}\n", .{try ds.columnMode("age")});
    
    // Create a dataset where the mode is clearly defined
    const more_ages = [_]u32{ 25, 25, 35 }; // 25 appears twice, should be the mode
    var ds2 = Dataset.init(allocator);
    defer ds2.deinit();
    try ds2.addColumn("age", &more_ages);
    try stdout.print("Test dataset - Mode of [25, 25, 35]: {d:.2}\n\n", .{try ds2.columnMode("age")});
    
    // Import first CSV file
    var imported_ds = Dataset.init(allocator);
    defer imported_ds.deinit();
    try imported_ds.fromCSV("demo.csv");
    try stdout.print("Imported Dataset:\n", .{});
    try imported_ds.formatAsTable(stdout);
    
    // Import second CSV file into a separate dataset object
    var stats_ds = Dataset.init(allocator);
    defer stats_ds.deinit();
    try stats_ds.fromCSV("stats.csv");
    try stdout.print("\nImported Stats Dataset:\n", .{});
    try stats_ds.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // Read and display the airports.csv file from the test_data directory
    try stdout.print("\nReading airports.csv from test_data directory...\n", .{});
    var airports_ds = Dataset.init(allocator);
    defer airports_ds.deinit();
    
    try airports_ds.fromCSV("test_data/airports.csv");
    
    // Display column information
    try stdout.print("Airports Dataset Info:\n", .{});
    try stdout.print("Number of rows: {d}\n", .{airports_ds.len()});
    
    const column_names = airports_ds.columnNames();
    defer allocator.free(column_names);
    
    try stdout.print("Number of columns: {d}\n", .{column_names.len});
    try stdout.print("Column names: {s}\n\n", .{column_names});
    
    // Print only the first 5 rows to avoid overwhelming output
    try stdout.print("First 5 rows of the airports dataset:\n", .{});
    
    // Create a smaller dataset with just the first 5 rows for display
    var display_ds = Dataset.init(allocator);
    defer display_ds.deinit();
    
    // Copy first 5 rows or fewer if the dataset is smaller
    const row_count = @min(airports_ds.len(), 5);
    
    // Copy each column from the original dataset to the display dataset
    for (column_names) |col_name| {
        try airports_ds.copyColumnTo(&display_ds, col_name, 0, row_count);
    }
    
    // Format the smaller dataset as a table
    try display_ds.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // Demonstrate filtering functionality with a sample dataset
    try stdout.print("\n==== Dataset Filtering Demonstration ====\n\n", .{});
    
    // Create a sample dataset with employee data
    try stdout.print("Creating a sample employee dataset...\n", .{});
    var employees = Dataset.init(allocator);
    defer employees.deinit();
    
    const emp_ids = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const emp_names = [_][]const u8{ "John Smith", "Jane Doe", "Bob Johnson", "Alice Williams", "Charlie Brown", 
                                   "Diana Prince", "Bruce Wayne", "Clark Kent", "Peter Parker", "Tony Stark" };
    const emp_departments = [_][]const u8{ "Engineering", "HR", "Engineering", "Marketing", "Sales", 
                                        "Engineering", "Finance", "HR", "Engineering", "Sales" };
    const emp_salaries = [_]f32{ 75000.0, 65000.0, 78000.0, 67000.0, 55000.0, 
                               85000.0, 95000.0, 62000.0, 72000.0, 81000.0 };
    const emp_ages = [_]i32{ 32, 28, 45, 36, 52, 29, 41, 33, 27, 38 };
    const emp_is_manager = [_]bool{ true, false, true, false, true, false, true, false, false, true };
    const emp_start_dates = [_][]const u8{ "2018-05-12", "2019-03-15", "2015-11-04", "2020-01-20", "2010-08-30", 
                                        "2021-02-15", "2017-07-22", "2019-09-01", "2022-04-10", "2016-12-05" };
    const emp_performance = [_]?f32{ 4.5, 3.8, null, 4.2, 3.5, 4.7, null, 3.9, 4.0, 4.8 };
    
    try employees.addColumn("id", &emp_ids);
    try employees.addColumn("name", &emp_names);
    try employees.addColumn("department", &emp_departments);
    try employees.addColumn("salary", &emp_salaries);
    try employees.addColumn("age", &emp_ages);
    try employees.addColumn("is_manager", &emp_is_manager);
    try employees.addColumn("start_date", &emp_start_dates);
    try employees.addColumn("performance", &emp_performance);
    
    try stdout.print("Employee dataset created with {d} rows and {d} columns\n\n", .{employees.len(), 8});
    try stdout.print("Full employee dataset:\n", .{});
    try employees.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 1. Filter employees by department (equals)
    try stdout.print("Filtering employees in Engineering department...\n", .{});
    var eng_employees = try employees.filter("department", libdata.ComparisonOp.Equal, "Engineering");
    defer eng_employees.deinit();
    try stdout.print("Found {d} employees in Engineering\n\n", .{eng_employees.len()});
    try stdout.print("Engineering employees:\n", .{});
    try eng_employees.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 2. Filter employees by salary (greater than)
    try stdout.print("Filtering employees with salary > 70000...\n", .{});
    var high_salary = try employees.filter("salary", libdata.ComparisonOp.GreaterThan, 70000.0);
    defer high_salary.deinit();
    try stdout.print("Found {d} employees with salary > 70000\n\n", .{high_salary.len()});
    try stdout.print("High salary employees:\n", .{});
    try high_salary.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 3. Filter employees by name (contains)
    try stdout.print("Filtering employees with 'an' in their name...\n", .{});
    var an_employees = try employees.filter("name", libdata.ComparisonOp.Contains, "an");
    defer an_employees.deinit();
    try stdout.print("Found {d} employees with 'an' in their name\n\n", .{an_employees.len()});
    try stdout.print("Employees with 'an' in their name:\n", .{});
    try an_employees.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 4. Combine filters using logical operations
    try stdout.print("Combining filters: Engineering employees with salary > 70000...\n", .{});
    var eng_high_salary = try libdata.filter.logicalAnd(&eng_employees, &high_salary);
    defer eng_high_salary.deinit();
    try stdout.print("Found {d} engineering employees with salary > 70000\n\n", .{eng_high_salary.len()});
    try stdout.print("Engineering employees with high salary:\n", .{});
    try eng_high_salary.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 5. Filter by boolean value
    try stdout.print("Filtering managers...\n", .{});
    var managers = try employees.filter("is_manager", libdata.ComparisonOp.Equal, true);
    defer managers.deinit();
    try stdout.print("Found {d} managers\n\n", .{managers.len()});
    try stdout.print("Managers:\n", .{});
    try managers.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 6. Filter by nullable value (not null)
    try stdout.print("Filtering employees with performance ratings...\n", .{});
    var rated_employees = try employees.filter("performance", libdata.ComparisonOp.IsNotNull, {});
    defer rated_employees.deinit();
    try stdout.print("Found {d} employees with performance ratings\n\n", .{rated_employees.len()});
    try stdout.print("Employees with performance ratings:\n", .{});
    try rated_employees.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    // 7. Custom predicate filter
    try stdout.print("Using custom predicate: Engineering employees who are not managers...\n", .{});
    
    const predicate = struct {
        fn apply(row_idx: usize, data_set: *const Dataset) bool {
            const department = data_set.getColumnValue(row_idx, "department", []const u8) catch return false;
            const is_manager = data_set.getColumnValue(row_idx, "is_manager", bool) catch return false;
            
            return std.mem.eql(u8, department, "Engineering") and !is_manager;
        }
    }.apply;
    
    var eng_non_managers = try employees.filterWithPredicate(predicate);
    defer eng_non_managers.deinit();
    try stdout.print("Found {d} engineering employees who are not managers\n\n", .{eng_non_managers.len()});
    try stdout.print("Engineering non-managers:\n", .{});
    try eng_non_managers.formatAsTable(stdout);
    try stdout.print("\n", .{});
    
    try stdout.print("==== End of Dataset Filtering Demonstration ====\n\n", .{});
    
    // Demonstrate streaming functionality
    try stdout.print("==== Dataset Streaming Demonstration ====\n\n", .{});
    
    // Clean up any leftover CSV files from previous runs
    std.fs.cwd().deleteFile("large_dataset.csv") catch {};
    std.fs.cwd().deleteFile("test_stream.csv") catch {};
    
    // Create a sample dataset for streaming demonstration
    try stdout.print("Creating a sample dataset for streaming...\n", .{});
    var stream_ds = Dataset.init(allocator);
    defer stream_ds.deinit();
    
    // Add some test data - similar to what we have in the employees dataset
    const stream_ids = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    const stream_names = [_][]const u8{ 
        "John", "Jane", "Bob", "Alice", "Charlie", 
        "Diana", "Bruce", "Clark", "Peter", "Tony",
        "Steve", "Natasha", "Thor", "Carol", "Scott"
    };
    const stream_values = [_]f32{ 
        75.5, 65.2, 82.1, 45.7, 90.3, 
        55.8, 72.4, 88.9, 63.1, 77.6,
        81.2, 68.7, 93.5, 59.8, 84.6
    };
    
    try stream_ds.addColumn("id", &stream_ids);
    try stream_ds.addColumn("name", &stream_names);
    try stream_ds.addColumn("value", &stream_values);
    
    try stdout.print("Created dataset with {d} rows\n\n", .{stream_ds.len()});
    
    // Demonstrate manual chunking as a simple form of streaming
    try stdout.print("Demonstrating manual chunking (simulated streaming)...\n", .{});
    
    // Define our chunk size
    const chunk_size: usize = 5;
    const total_chunks = (stream_ds.len() + chunk_size - 1) / chunk_size; // Ceiling division
    
    var total_rows: usize = 0;
    var sum_of_values: f32 = 0.0;
    
    // Process the dataset in chunks manually
    for (0..total_chunks) |chunk_idx| {
        // Calculate start and end indices for this chunk
        const start_idx = chunk_idx * chunk_size;
        const end_idx = @min(start_idx + chunk_size, stream_ds.len());
        const chunk_rows = end_idx - start_idx;
        
        // Process this chunk directly without creating a new dataset
        total_rows += chunk_rows;
        
        // Calculate sum of values in this chunk
        for (start_idx..end_idx) |i| {
            const value = try stream_ds.getColumnValue(i, "value", f32);
            sum_of_values += value;
        }
        
        try stdout.print("Processed chunk {d} with {d} rows\n", .{chunk_idx + 1, chunk_rows});
    }
    
    const avg_value = sum_of_values / @as(f32, @floatFromInt(total_rows));
    
    try stdout.print("\nChunking results:\n", .{});
    try stdout.print("Total chunks processed: {d}\n", .{total_chunks});
    try stdout.print("Total rows processed: {d}\n", .{total_rows});
    try stdout.print("Average value: {d:.2}\n\n", .{avg_value});
    
    // Demonstrate filtering with manual chunking
    try stdout.print("Filtering data with manual chunking...\n", .{});
    
    // Create arrays to hold filtered results
    var filtered_ids = std.ArrayList(i32).init(allocator);
    defer filtered_ids.deinit();
    var filtered_names = std.ArrayList([]const u8).init(allocator);
    defer filtered_names.deinit();
    var filtered_values = std.ArrayList(f32).init(allocator);
    defer filtered_values.deinit();
    
    // Process the dataset in chunks manually and filter
    for (0..total_chunks) |chunk_idx| {
        // Calculate start and end indices for this chunk
        const start_idx = chunk_idx * chunk_size;
        const end_idx = @min(start_idx + chunk_size, stream_ds.len());
        
        // Process each row in the chunk
        for (start_idx..end_idx) |row_idx| {
            const value = try stream_ds.getColumnValue(row_idx, "value", f32);
            
            // Filter for high values (> 75)
            if (value > 75.0) {
                const id = try stream_ds.getColumnValue(row_idx, "id", i32);
                const name = try stream_ds.getColumnValue(row_idx, "name", []const u8);
                
                // Add to filtered arrays
                try filtered_ids.append(id);
                try filtered_names.append(name);
                try filtered_values.append(value);
            }
        }
    }
    
    // Create a new dataset with the filtered data
    var high_values = Dataset.init(allocator);
    defer high_values.deinit();
    
    if (filtered_ids.items.len > 0) {
        try high_values.addColumn("id", filtered_ids.items);
        try high_values.addColumn("name", filtered_names.items);
        try high_values.addColumn("value", filtered_values.items);
    }
    
    try stdout.print("Found {d} rows with values > 75\n", .{high_values.rows});
    
    // Show the filtered data
    try stdout.print("\nFiltered data (values > 75):\n", .{});
    try high_values.formatAsTable(stdout);
    
    try stdout.print("\n==== End of Dataset Streaming Demonstration ====\n\n", .{});
    
    // Clean up any CSV files created during the demonstration
    std.fs.cwd().deleteFile("large_dataset.csv") catch {};
    std.fs.cwd().deleteFile("test_stream.csv") catch {};
    
    // Run the tensor demonstration
    try demonstrateTensor(allocator, stdout);
}
