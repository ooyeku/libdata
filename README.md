# LibData

A dataset library for Zig.


## Installation

Requires Zig 0.13.0 or later.

```bash
# Clone the repository
git clone https://github.com/ooyeku/libdata.git
cd libdata

# Build the project
zig build
```

## Usage Example

```zig
const std = @import("std");
const libdata = @import("root.zig");

pub fn main() !void {
    // Initialize the dataset
    const allocator = std.heap.page_allocator;
    var ds = libdata.Dataset.init(allocator);
    defer ds.deinit();

    // Add data columns
    const names = [_][]const u8{ "Alice", "Bob", "Charlie" };
    const ages = [_]u32{ 25, 30, 35 };
    
    try ds.addColumn("name", &names);
    try ds.addColumn("age", &ages);

    // Print the dataset as a formatted table
    const stdout = std.io.getStdOut().writer();
    try ds.formatAsTable(stdout);
}
```

## Project Structure

```
libdata/
├── src/
│   ├── main.zig      # Example usage and tests
│   ├── dataset.zig   # Dataset implementation
│   └── tensor.zig    # Tensor operations
├── build.zig         # Build system
└── build.zig.zon     # Dependencies
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

