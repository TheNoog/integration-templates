const std = @import("std");

pub fn main() !void {
    const url = "https://randomuser.me/api/";
    const file_path = "data.json";

    var allocator = std.heap.page_allocator;
    const http_client = try std.net.http.Client.init(allocator);

    const response = try http_client.get(url, allocator);

    if (response.status_code == 200) {
        const body = try response.getBody(allocator);

        const file = try std.fs.cwd().createFile(file_path) catch |err| {
            std.debug.print("Error creating file: {}\n", .{err});
            return error.Err;
        };

        try file.writeAll(body);
        std.debug.print("File saved successfully.\n");
    } else {
        std.debug.print("Error requesting URL: {}\n", .{response.status_code});
    }
}
