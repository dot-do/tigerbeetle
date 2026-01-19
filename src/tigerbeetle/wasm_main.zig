// TigerBeetle WASM Entry Point
//
// This is a stripped-down entry point for WASM builds that exposes
// the core state machine and storage functionality without the CLI
// and networking code that depends on POSIX types.
//
// For now, this is a minimal stub that proves the WASM build works.
// Full implementation requires connecting the state machine to the
// JavaScript VFS bridge.

const std = @import("std");
const builtin = @import("builtin");

// Import time from JavaScript VFS
extern "env" fn vfs_time_monotonic() u64;

// WASM exports for JavaScript interop
// These are the core operations exposed to the JavaScript VFS bridge

/// Initialize the state machine with given storage
export fn tb_init() callconv(.C) i32 {
    // TODO: Initialize state machine
    return 0;
}

/// Create accounts batch
/// Returns 0 on success, negative on error
export fn tb_create_accounts(
    accounts_ptr: [*]const u8, // Raw bytes of Account structs
    accounts_len: u32,
    results_ptr: [*]u8, // Raw bytes for results
    results_len: *u32,
) callconv(.C) i32 {
    _ = accounts_ptr;
    _ = accounts_len;
    _ = results_ptr;
    _ = results_len;
    // TODO: Implement create accounts
    return 0;
}

/// Create transfers batch
export fn tb_create_transfers(
    transfers_ptr: [*]const u8,
    transfers_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    _ = transfers_ptr;
    _ = transfers_len;
    _ = results_ptr;
    _ = results_len;
    // TODO: Implement create transfers
    return 0;
}

/// Lookup accounts by ID
export fn tb_lookup_accounts(
    ids_ptr: [*]const u128,
    ids_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    _ = ids_ptr;
    _ = ids_len;
    _ = results_ptr;
    _ = results_len;
    // TODO: Implement lookup accounts
    return 0;
}

/// Lookup transfers by ID
export fn tb_lookup_transfers(
    ids_ptr: [*]const u128,
    ids_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    _ = ids_ptr;
    _ = ids_len;
    _ = results_ptr;
    _ = results_len;
    // TODO: Implement lookup transfers
    return 0;
}

/// Get transfers for an account
export fn tb_get_account_transfers(
    account_id: u128,
    results_ptr: [*]u8,
    results_max: u32,
    results_len: *u32,
) callconv(.C) i32 {
    _ = account_id;
    _ = results_ptr;
    _ = results_max;
    _ = results_len;
    // TODO: Implement get account transfers
    return 0;
}

/// Tick the state machine (process pending operations)
export fn tb_tick() callconv(.C) void {
    // TODO: Process pending operations
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
