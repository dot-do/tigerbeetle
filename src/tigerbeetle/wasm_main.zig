// TigerBeetle WASM Entry Point
//
// This is a stripped-down entry point for WASM builds that exposes
// the core state machine functionality through a simple in-memory
// implementation suitable for Cloudflare Workers.
//
// The implementation uses a fixed-size array for account storage
// rather than the full LSM-tree based storage system, making it
// suitable for embedded/WASM environments with limited resources.

const std = @import("std");
const builtin = @import("builtin");
const math = std.math;

const vsr = @import("vsr");
const tb = vsr.tigerbeetle;

// Re-export types from tigerbeetle.zig
const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const CreateAccountResult = tb.CreateAccountResult;
const CreateAccountsResult = tb.CreateAccountsResult;

// Import time from JavaScript VFS
extern "env" fn vfs_time_monotonic() u64;

// Import VFS persistence callbacks from JavaScript
// These allow the DO to save/restore state on each request
extern "env" fn vfs_read(fd: i32, buf_ptr: [*]u8, buf_len: u32, offset: u64) i32;
extern "env" fn vfs_write(fd: i32, buf_ptr: [*]const u8, buf_len: u32, offset: u64) i32;

// =============================================================================
// In-Memory State Machine
// =============================================================================
//
// A simplified state machine that stores accounts in a fixed-size array.
// This avoids the complexity of the LSM-tree storage while providing
// the core TigerBeetle accounting semantics.

const MAX_ACCOUNTS = 10000;

const InMemoryStateMachine = struct {
    accounts: [MAX_ACCOUNTS]Account = undefined,
    account_count: u32 = 0,
    // Index for fast ID lookups - maps account ID to slot index
    // Using a simple linear search for now; could be optimized with a hash map
    commit_timestamp: u64 = 0,
    initialized: bool = false,

    const Self = @This();

    fn init(self: *Self) void {
        self.account_count = 0;
        self.commit_timestamp = 0;
        self.initialized = true;
    }

    /// Find an account by ID using linear search
    /// Returns the index if found, null otherwise
    fn findAccountIndex(self: *const Self, id: u128) ?u32 {
        var i: u32 = 0;
        while (i < self.account_count) : (i += 1) {
            if (self.accounts[i].id == id) {
                return i;
            }
        }
        return null;
    }

    /// Get an account by ID
    fn getAccount(self: *const Self, id: u128) ?*const Account {
        if (self.findAccountIndex(id)) |index| {
            return &self.accounts[index];
        }
        return null;
    }

    /// Create a new account with validation
    /// Returns the result code (ok = 0, or an error code)
    fn createAccount(self: *Self, timestamp: u64, a: *const Account) CreateAccountResult {
        // Validate reserved fields
        if (a.reserved != 0) return .reserved_field;
        if (a.flags.padding != 0) return .reserved_flag;

        // Validate ID
        if (a.id == 0) return .id_must_not_be_zero;
        if (a.id == math.maxInt(u128)) return .id_must_not_be_int_max;

        // Check if account already exists
        if (self.getAccount(a.id)) |existing| {
            return createAccountExists(a, existing);
        }

        // Validate flag combinations
        if (a.flags.debits_must_not_exceed_credits and a.flags.credits_must_not_exceed_debits) {
            return .flags_are_mutually_exclusive;
        }

        // Validate initial balances must be zero
        if (a.debits_pending != 0) return .debits_pending_must_be_zero;
        if (a.debits_posted != 0) return .debits_posted_must_be_zero;
        if (a.credits_pending != 0) return .credits_pending_must_be_zero;
        if (a.credits_posted != 0) return .credits_posted_must_be_zero;

        // Validate required fields
        if (a.ledger == 0) return .ledger_must_not_be_zero;
        if (a.code == 0) return .code_must_not_be_zero;

        // Check capacity
        if (self.account_count >= MAX_ACCOUNTS) {
            // No specific error for capacity, return a generic error
            // This shouldn't happen in normal use cases
            return .reserved_field;
        }

        // Insert the account
        self.accounts[self.account_count] = Account{
            .id = a.id,
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
            .user_data_128 = a.user_data_128,
            .user_data_64 = a.user_data_64,
            .user_data_32 = a.user_data_32,
            .reserved = 0,
            .ledger = a.ledger,
            .code = a.code,
            .flags = a.flags,
            .timestamp = timestamp,
        };
        self.account_count += 1;
        self.commit_timestamp = timestamp;

        return .ok;
    }

    /// Check if an account already exists and return appropriate error
    fn createAccountExists(a: *const Account, e: *const Account) CreateAccountResult {
        if (@as(u16, @bitCast(a.flags)) != @as(u16, @bitCast(e.flags))) {
            return .exists_with_different_flags;
        }
        if (a.user_data_128 != e.user_data_128) return .exists_with_different_user_data_128;
        if (a.user_data_64 != e.user_data_64) return .exists_with_different_user_data_64;
        if (a.user_data_32 != e.user_data_32) return .exists_with_different_user_data_32;
        if (a.ledger != e.ledger) return .exists_with_different_ledger;
        if (a.code != e.code) return .exists_with_different_code;
        return .exists;
    }
};

// Global state machine instance
var state_machine: InMemoryStateMachine = .{};

// =============================================================================
// WASM Exports
// =============================================================================

/// Initialize the state machine
export fn tb_init() callconv(.C) i32 {
    state_machine.init();
    return 0;
}

/// Create accounts batch
/// Returns 0 on success, negative on error
/// The results buffer is populated with CreateAccountsResult structs for any errors
export fn tb_create_accounts(
    accounts_ptr: [*]const u8,
    accounts_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    // Calculate number of accounts
    const account_size = @sizeOf(Account);
    if (accounts_len % account_size != 0) {
        return -2; // Invalid input size
    }
    const num_accounts = accounts_len / account_size;

    // Cast input to Account slice
    const accounts = @as([*]const Account, @ptrCast(@alignCast(accounts_ptr)))[0..num_accounts];

    // Cast output to results slice
    const result_size = @sizeOf(CreateAccountsResult);
    const max_results = results_len.* / result_size;
    const results = @as([*]CreateAccountsResult, @ptrCast(@alignCast(results_ptr)))[0..max_results];

    var results_count: u32 = 0;
    const base_timestamp = vfs_time_monotonic();

    for (accounts, 0..) |*account, i| {
        // Each account gets a unique timestamp
        const timestamp = base_timestamp + @as(u64, @intCast(i));
        const result = state_machine.createAccount(timestamp, account);

        // Only record errors (ok results are not included in output)
        if (result != .ok) {
            if (results_count < max_results) {
                results[results_count] = CreateAccountsResult{
                    .index = @as(u32, @intCast(i)),
                    .result = result,
                };
                results_count += 1;
            }
        }
    }

    results_len.* = results_count * result_size;
    return 0;
}

/// Create transfers batch (stub - not implemented yet)
export fn tb_create_transfers(
    transfers_ptr: [*]const u8,
    transfers_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    _ = transfers_ptr;
    _ = transfers_len;
    _ = results_ptr;
    results_len.* = 0;
    // TODO: Implement create transfers
    return -100; // Not implemented
}

/// Lookup accounts by ID
/// Returns 0 on success, negative on error
/// The results buffer is populated with Account structs for found accounts
export fn tb_lookup_accounts(
    ids_ptr: [*]const u128,
    ids_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    const ids = ids_ptr[0..ids_len];

    // Cast output to Account slice
    const account_size = @sizeOf(Account);
    const max_results = results_len.* / account_size;
    const results = @as([*]Account, @ptrCast(@alignCast(results_ptr)))[0..max_results];

    var results_count: u32 = 0;

    for (ids) |id| {
        if (state_machine.getAccount(id)) |account| {
            if (results_count < max_results) {
                results[results_count] = account.*;
                results_count += 1;
            }
        }
        // Accounts not found are simply omitted from results
    }

    results_len.* = results_count * account_size;
    return 0;
}

/// Lookup transfers by ID (stub - not implemented yet)
export fn tb_lookup_transfers(
    ids_ptr: [*]const u128,
    ids_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    _ = ids_ptr;
    _ = ids_len;
    _ = results_ptr;
    results_len.* = 0;
    // TODO: Implement lookup transfers
    return -100; // Not implemented
}

/// Get transfers for an account (stub - not implemented yet)
export fn tb_get_account_transfers(
    account_id: u128,
    results_ptr: [*]u8,
    results_max: u32,
    results_len: *u32,
) callconv(.C) i32 {
    _ = account_id;
    _ = results_ptr;
    _ = results_max;
    results_len.* = 0;
    // TODO: Implement get account transfers
    return -100; // Not implemented
}

/// Tick the state machine (no-op for in-memory implementation)
export fn tb_tick() callconv(.C) void {
    // No-op for simple in-memory state machine
}

/// Get the current timestamp from JavaScript
export fn tb_timestamp() callconv(.C) u64 {
    return vfs_time_monotonic();
}

/// Get version info
export fn tb_version() callconv(.C) u32 {
    // Version 0.1.0 as packed u32
    return (0 << 16) | (1 << 8) | 0;
}

/// Get current account count (for debugging/testing)
export fn tb_account_count() callconv(.C) u32 {
    return state_machine.account_count;
}

/// Memory allocation for WASM - expose allocator to JavaScript
var wasm_allocator: ?std.mem.Allocator = null;
var wasm_buffer: [64 * 1024]u8 = undefined; // 64KB fixed buffer
var fba: std.heap.FixedBufferAllocator = undefined;

export fn tb_alloc(size: u32) callconv(.C) ?[*]u8 {
    if (wasm_allocator == null) {
        fba = std.heap.FixedBufferAllocator.init(&wasm_buffer);
        wasm_allocator = fba.allocator();
    }

    const slice = wasm_allocator.?.alloc(u8, size) catch return null;
    return slice.ptr;
}

export fn tb_free(ptr: [*]u8, size: u32) callconv(.C) void {
    if (wasm_allocator) |allocator| {
        allocator.free(ptr[0..size]);
    }
}

// =============================================================================
// State Persistence
// =============================================================================
//
// These functions allow the Durable Object to save and restore state
// between requests, enabling persistence through the VFS callbacks.

/// State header for serialization
const StateHeader = packed struct {
    magic: u32 = 0x54425354, // "TBST" - TigerBeetle State
    version: u32 = 1,
    account_count: u32,
    commit_timestamp: u64,
    reserved: u64 = 0,
};

/// Calculate the size needed to serialize current state
export fn tb_state_size() callconv(.C) u32 {
    const header_size = @sizeOf(StateHeader);
    const accounts_size = state_machine.account_count * @sizeOf(Account);
    return @as(u32, header_size + accounts_size);
}

/// Save state to a buffer
/// Returns 0 on success, negative on error
/// buf_len should be at least tb_state_size() bytes
export fn tb_save_state(buf_ptr: [*]u8, buf_len: u32) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    const required_size = tb_state_size();
    if (buf_len < required_size) {
        return -2; // Buffer too small
    }

    // Write header
    const header = StateHeader{
        .account_count = state_machine.account_count,
        .commit_timestamp = state_machine.commit_timestamp,
    };
    const header_bytes = @as([*]const u8, @ptrCast(&header));
    @memcpy(buf_ptr[0..@sizeOf(StateHeader)], header_bytes[0..@sizeOf(StateHeader)]);

    // Write accounts
    if (state_machine.account_count > 0) {
        const accounts_offset = @sizeOf(StateHeader);
        const accounts_size = state_machine.account_count * @sizeOf(Account);
        const accounts_bytes = @as([*]const u8, @ptrCast(&state_machine.accounts));
        @memcpy(buf_ptr[accounts_offset .. accounts_offset + accounts_size], accounts_bytes[0..accounts_size]);
    }

    return 0;
}

/// Load state from a buffer
/// Returns 0 on success, negative on error
export fn tb_load_state(buf_ptr: [*]const u8, buf_len: u32) callconv(.C) i32 {
    const header_size = @sizeOf(StateHeader);

    if (buf_len < header_size) {
        return -1; // Buffer too small for header
    }

    // Read header
    const header = @as(*const StateHeader, @ptrCast(@alignCast(buf_ptr))).*;

    // Validate magic
    if (header.magic != 0x54425354) {
        return -2; // Invalid magic
    }

    // Validate version
    if (header.version != 1) {
        return -3; // Unsupported version
    }

    // Validate account count
    if (header.account_count > MAX_ACCOUNTS) {
        return -4; // Too many accounts
    }

    // Validate buffer size
    const expected_size = header_size + header.account_count * @sizeOf(Account);
    if (buf_len < expected_size) {
        return -5; // Buffer too small for accounts
    }

    // Load accounts
    if (header.account_count > 0) {
        const accounts_offset = header_size;
        const accounts_size = header.account_count * @sizeOf(Account);
        const accounts_bytes = @as([*]u8, @ptrCast(&state_machine.accounts));
        @memcpy(accounts_bytes[0..accounts_size], buf_ptr[accounts_offset .. accounts_offset + accounts_size]);
    }

    state_machine.account_count = header.account_count;
    state_machine.commit_timestamp = header.commit_timestamp;
    state_machine.initialized = true;

    return 0;
}

/// Save state directly to VFS file descriptor
/// This is a convenience function that combines tb_save_state with vfs_write
/// Returns 0 on success, negative on error
export fn tb_persist_state(fd: i32) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    const state_size = tb_state_size();

    // Use the wasm_buffer for serialization
    if (state_size > wasm_buffer.len) {
        return -2; // State too large for buffer
    }

    // Serialize to buffer
    const save_result = tb_save_state(&wasm_buffer, @as(u32, wasm_buffer.len));
    if (save_result != 0) {
        return save_result;
    }

    // Write to VFS
    const write_result = vfs_write(fd, &wasm_buffer, state_size, 0);
    if (write_result < 0) {
        return -3; // VFS write error
    }

    return 0;
}

/// Load state directly from VFS file descriptor
/// This is a convenience function that combines vfs_read with tb_load_state
/// Returns 0 on success, negative on error
export fn tb_restore_state(fd: i32, max_size: u32) callconv(.C) i32 {
    const read_size = if (max_size > wasm_buffer.len) @as(u32, wasm_buffer.len) else max_size;

    // Read from VFS
    const bytes_read = vfs_read(fd, &wasm_buffer, read_size, 0);
    if (bytes_read < 0) {
        return -1; // VFS read error
    }
    if (bytes_read == 0) {
        // No existing state - initialize fresh
        state_machine.init();
        return 0;
    }

    // Load from buffer
    return tb_load_state(&wasm_buffer, @as(u32, @intCast(bytes_read)));
}
