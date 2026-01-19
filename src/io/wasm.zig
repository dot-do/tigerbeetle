// TigerBeetle WASM IO Implementation
//
// This module provides async I/O for TigerBeetle running in WebAssembly.
// It bridges Zig code to JavaScript VFS functions using extern imports.
//
// Design:
// - All I/O is delegated to JavaScript via imported functions
// - Uses Asyncify for yielding to JS event loop during async operations
// - No direct file system access - everything goes through the VFS bridge
//
// Build with: zig build -Dtarget=wasm32-freestanding
// Must be linked with Asyncify-enabled JavaScript loader

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.io);

const stdx = @import("stdx");
const constants = @import("../constants.zig");
const common = @import("./common.zig");
const QueueType = @import("../queue.zig").QueueType;
const buffer_limit = @import("../io.zig").buffer_limit;
const DirectIO = @import("../io.zig").DirectIO;

// ============================================================================
// JavaScript VFS Imports
// These functions are provided by the JavaScript runtime
// ============================================================================

extern "env" fn vfs_read(fd: i32, buf_ptr: [*]u8, buf_len: u32, offset: u64) i32;
extern "env" fn vfs_write(fd: i32, buf_ptr: [*]const u8, buf_len: u32, offset: u64) i32;
extern "env" fn vfs_open(path_ptr: [*]const u8, path_len: u32, flags: u32) i32;
extern "env" fn vfs_close(fd: i32) i32;
extern "env" fn vfs_fsync(fd: i32) i32;
extern "env" fn vfs_ftruncate(fd: i32, size: u64) i32;
extern "env" fn vfs_file_size(fd: i32) i64;
extern "env" fn vfs_time_monotonic() u64;
extern "env" fn vfs_yield() void;

// Error codes from JavaScript
const VFS_OK: i32 = 0;
const VFS_ERR_IO: i32 = -1;
const VFS_ERR_NOT_FOUND: i32 = -2;
const VFS_ERR_INVALID: i32 = -3;
const VFS_ERR_FULL: i32 = -4;

pub const IO = struct {
    pub const TCPOptions = common.TCPOptions;
    pub const ListenOptions = common.ListenOptions;

    // No file descriptor type in WASM - we use i32 handles
    pub const fd_t = i32;
    pub const socket_t = i32;
    pub const INVALID_FILE: fd_t = -1;

    // Time tracking for timeouts
    time_base: u64 = 0,

    // Completion queues
    timeouts: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_timeouts" }),
    completed: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_completed" }),
    io_pending: QueueType(Completion) = QueueType(Completion).init(.{ .name = "io_pending" }),

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;
        return IO{
            .time_base = vfs_time_monotonic(),
        };
    }

    pub fn deinit(self: *IO) void {
        _ = self;
        // No cleanup needed for WASM
    }

    /// Pass all queued submissions and peek for completions.
    pub fn run(self: *IO) !void {
        return self.flush(false);
    }

    /// Pass all queued submissions and run for `nanoseconds`.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        var timed_out = false;
        var completion: Completion = undefined;

        const on_timeout = struct {
            fn callback(
                timed_out_ptr: *bool,
                _completion: *Completion,
                result: TimeoutError!void,
            ) void {
                _ = _completion;
                _ = result catch unreachable;
                timed_out_ptr.* = true;
            }
        }.callback;

        self.timeout(
            *bool,
            &timed_out,
            on_timeout,
            &completion,
            nanoseconds,
        );

        while (!timed_out) {
            try self.flush(true);
            // Yield to JavaScript event loop
            vfs_yield();
        }
    }

    fn flush(self: *IO, wait_for_completions: bool) !void {
        // Process pending I/O
        const next_timeout = self.flush_timeouts();
        _ = self.flush_io();

        if (wait_for_completions and self.completed.empty()) {
            if (next_timeout) |timeout_ns| {
                // In WASM, we yield to JS and let it handle the timeout
                _ = timeout_ns;
                vfs_yield();
            }
        }

        // Run completion callbacks
        var completed = self.completed;
        self.completed.reset();
        while (completed.pop()) |completion| {
            (completion.callback)(self, completion);
        }
    }

    fn flush_io(self: *IO) usize {
        var flushed: usize = 0;
        while (self.io_pending.pop()) |completion| {
            // Execute the I/O operation synchronously (JS handles async via Asyncify)
            const result = switch (completion.operation) {
                .read => |op| blk: {
                    const bytes = vfs_read(op.fd, op.buf, op.len, op.offset);
                    break :blk if (bytes >= 0) @as(usize, @intCast(bytes)) else error.InputOutput;
                },
                .write => |op| blk: {
                    const bytes = vfs_write(op.fd, op.buf, op.len, op.offset);
                    break :blk if (bytes >= 0) @as(usize, @intCast(bytes)) else error.InputOutput;
                },
                .fsync => |op| blk: {
                    const ret = vfs_fsync(op.fd);
                    break :blk if (ret == VFS_OK) {} else error.InputOutput;
                },
                .close => |op| blk: {
                    const ret = vfs_close(op.fd);
                    break :blk if (ret == VFS_OK) {} else error.InputOutput;
                },
                else => continue,
            };

            // Store result and queue for completion
            completion.result = result;
            self.completed.push(completion);
            flushed += 1;
        }
        return flushed;
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_timeout: ?u64 = null;
        var timeouts_iterator = self.timeouts.iterate();

        while (timeouts_iterator.next()) |completion| {
            const now = vfs_time_monotonic();
            const expires = completion.operation.timeout.expires;

            if (now >= expires) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            const timeout_ns = expires - now;
            if (min_timeout) |min_ns| {
                min_timeout = @min(min_ns, timeout_ns);
            } else {
                min_timeout = timeout_ns;
            }
        }
        return min_timeout;
    }

    /// Completion structure for async operations
    pub const Completion = struct {
        link: QueueType(Completion).Link = .{},
        context: ?*anyopaque,
        callback: *const fn (*IO, *Completion) void,
        operation: Operation,
        result: anyerror!usize = error.NotStarted,
    };

    const Operation = union(enum) {
        accept: struct {
            socket: socket_t,
        },
        close: struct {
            fd: fd_t,
        },
        connect: struct {
            socket: socket_t,
            address: std.net.Address,
            initiated: bool,
        },
        fsync: struct {
            fd: fd_t,
        },
        read: struct {
            fd: fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
        },
        recv: struct {
            socket: socket_t,
            buf: [*]u8,
            len: u32,
        },
        send: struct {
            socket: socket_t,
            buf: [*]const u8,
            len: u32,
        },
        timeout: struct {
            expires: u64,
        },
        write: struct {
            fd: fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
        },
    };

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime operation_tag: std.meta.Tag(Operation),
        operation_data: std.meta.TagPayload(Operation, operation_tag),
    ) void {
        const on_complete_fn = struct {
            fn on_complete(_: *IO, _completion: *Completion) void {
                return callback(
                    @ptrCast(@alignCast(_completion.context)),
                    _completion,
                    _completion.result,
                );
            }
        }.on_complete;

        completion.* = .{
            .link = .{},
            .context = context,
            .callback = on_complete_fn,
            .operation = @unionInit(Operation, @tagName(operation_tag), operation_data),
        };

        switch (operation_tag) {
            .timeout => self.timeouts.push(completion),
            else => self.io_pending.push(completion),
        }
    }

    pub fn cancel_all(_: *IO) void {
        // TODO: Implement cancellation
    }

    pub const CancelError = error{
        NotRunning,
        NotInterruptable,
    };

    pub fn cancel(_: *IO, _: *Completion) CancelError!void {
        return error.NotInterruptable;
    }

    // ========================================================================
    // Public API - matches darwin.zig/linux.zig interface
    // ========================================================================

    pub const AcceptError = error{
        Unexpected,
        SocketNotListening,
        WouldBlock,
    };

    pub fn accept(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: AcceptError!socket_t,
        ) void,
        completion: *Completion,
        socket: socket_t,
    ) void {
        // Network accept not supported in WASM
        _ = self;
        _ = socket;
        callback(context, completion, error.Unexpected);
    }

    pub const ConnectError = error{
        Unexpected,
        WouldBlock,
        ConnectionRefused,
    };

    pub fn connect(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        socket: socket_t,
        address: std.net.Address,
    ) void {
        // Network connect not supported in WASM
        _ = self;
        _ = socket;
        _ = address;
        callback(context, completion, error.Unexpected);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
        ConnectionTimedOut,
        Unexpected,
    };

    pub fn read(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .read,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @intCast(buffer_limit(buffer.len)),
                .offset = offset,
            },
        );
    }

    pub const WriteError = error{
        WouldBlock,
        NotOpenForWriting,
        Alignment,
        InputOutput,
        LockViolation,
        NoSpaceLeft,
        FileTooBig,
        DiskQuota,
        InvalidArgument,
        BrokenPipe,
        SystemResources,
        Unexpected,
    };

    pub fn write(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .write,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @intCast(buffer_limit(buffer.len)),
                .offset = offset,
            },
        );
    }

    pub const RecvError = error{
        WouldBlock,
        ConnectionRefused,
        ConnectionResetByPeer,
        SystemResources,
        Unexpected,
    };

    pub fn recv(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        socket: socket_t,
        buffer: []u8,
    ) void {
        // Network recv not supported in WASM
        _ = self;
        _ = socket;
        _ = buffer;
        callback(context, completion, error.Unexpected);
    }

    pub const SendError = error{
        WouldBlock,
        ConnectionRefused,
        ConnectionResetByPeer,
        BrokenPipe,
        SystemResources,
        Unexpected,
    };

    pub fn send(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        socket: socket_t,
        buffer: []const u8,
    ) void {
        // Network send not supported in WASM
        _ = self;
        _ = socket;
        _ = buffer;
        callback(context, completion, error.Unexpected);
    }

    pub const TimeoutError = error{
        Canceled,
        Unexpected,
    };

    pub fn timeout(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        const now = vfs_time_monotonic();
        self.submit(
            context,
            callback,
            completion,
            .timeout,
            .{
                .expires = now + nanoseconds,
            },
        );
    }

    pub const CloseError = error{
        Unexpected,
    };

    pub fn close(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .close,
            .{ .fd = fd },
        );
    }

    pub const FsyncError = error{
        InputOutput,
        Unexpected,
    };

    pub fn fsync(
        self: *IO,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            completion: *Completion,
            result: FsyncError!void,
        ) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .fsync,
            .{ .fd = fd },
        );
    }

    // ========================================================================
    // File Operations
    // ========================================================================

    pub const OpenDataFilePurpose = enum { data, grid };

    pub fn open_dir(dirname: []const u8) !fd_t {
        // Directories are handled by the JS VFS
        _ = dirname;
        return 0; // Dummy directory FD
    }

    pub fn open_data_file(
        self: *IO,
        dir_fd: fd_t,
        basename: []const u8,
        size_min: u64,
        purpose: OpenDataFilePurpose,
        direct_io: DirectIO,
    ) !fd_t {
        _ = self;
        _ = dir_fd;
        _ = size_min;
        _ = purpose;
        _ = direct_io;

        // Open via JavaScript VFS
        const fd = vfs_open(basename.ptr, @intCast(basename.len), 0);
        if (fd < 0) {
            return switch (fd) {
                VFS_ERR_NOT_FOUND => error.FileNotFound,
                VFS_ERR_INVALID => error.InvalidArgument,
                else => error.InputOutput,
            };
        }
        return fd;
    }

    // ========================================================================
    // Socket Operations (not supported in WASM)
    // ========================================================================

    pub fn open_socket(self: *IO, options: ListenOptions) !socket_t {
        _ = self;
        _ = options;
        return error.Unexpected;
    }

    pub fn close_socket(self: *IO, fd: socket_t) void {
        _ = self;
        _ = fd;
    }

    pub fn listen(self: *IO, fd: socket_t, options: ListenOptions) !void {
        _ = self;
        _ = fd;
        _ = options;
        return error.Unexpected;
    }
};
