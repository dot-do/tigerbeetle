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
const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const CreateTransferResult = tb.CreateTransferResult;
const CreateTransfersResult = tb.CreateTransfersResult;

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
const MAX_TRANSFERS = 50000;
const MAX_PENDING_TRANSFERS = 10000;

/// Pending transfer tracking information
const PendingTransferInfo = struct {
    /// Original transfer ID
    id: u128,
    /// Amount from the original pending transfer
    original_amount: u128,
    /// Amount already posted (for partial posts)
    amount_posted: u128,
    /// Timestamp when expires (0 = never)
    expires_at: u64,
    /// State: 0 = active, 1 = posted, 2 = voided, 3 = expired
    state: u8,

    const STATE_ACTIVE: u8 = 0;
    const STATE_POSTED: u8 = 1;
    const STATE_VOIDED: u8 = 2;
    const STATE_EXPIRED: u8 = 3;
};

const InMemoryStateMachine = struct {
    accounts: [MAX_ACCOUNTS]Account = undefined,
    account_count: u32 = 0,
    transfers: [MAX_TRANSFERS]Transfer = undefined,
    transfer_count: u32 = 0,
    pending_transfers: [MAX_PENDING_TRANSFERS]PendingTransferInfo = undefined,
    pending_transfer_count: u32 = 0,
    // Index for fast ID lookups - maps account ID to slot index
    // Using a simple linear search for now; could be optimized with a hash map
    commit_timestamp: u64 = 0,
    initialized: bool = false,

    const Self = @This();

    fn init(self: *Self) void {
        self.account_count = 0;
        self.transfer_count = 0;
        self.pending_transfer_count = 0;
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

    // =========================================================================
    // Transfer Operations
    // =========================================================================

    /// Find a transfer by ID using linear search
    fn findTransferIndex(self: *const Self, id: u128) ?u32 {
        var i: u32 = 0;
        while (i < self.transfer_count) : (i += 1) {
            if (self.transfers[i].id == id) {
                return i;
            }
        }
        return null;
    }

    /// Get a transfer by ID
    fn getTransfer(self: *const Self, id: u128) ?*const Transfer {
        if (self.findTransferIndex(id)) |index| {
            return &self.transfers[index];
        }
        return null;
    }

    /// Find pending transfer info by transfer ID
    fn findPendingTransferIndex(self: *const Self, id: u128) ?u32 {
        var i: u32 = 0;
        while (i < self.pending_transfer_count) : (i += 1) {
            if (self.pending_transfers[i].id == id) {
                return i;
            }
        }
        return null;
    }

    /// Get pending transfer info by ID
    fn getPendingTransfer(self: *Self, id: u128) ?*PendingTransferInfo {
        if (self.findPendingTransferIndex(id)) |index| {
            return &self.pending_transfers[index];
        }
        return null;
    }

    /// Get mutable account by ID
    fn getAccountMut(self: *Self, id: u128) ?*Account {
        if (self.findAccountIndex(id)) |index| {
            return &self.accounts[index];
        }
        return null;
    }

    /// Create a new transfer with validation
    fn createTransfer(self: *Self, timestamp: u64, t: *const Transfer) CreateTransferResult {
        // Validate reserved flags
        if (t.flags.padding != 0) return .reserved_flag;

        // Validate ID
        if (t.id == 0) return .id_must_not_be_zero;
        if (t.id == math.maxInt(u128)) return .id_must_not_be_int_max;

        // Check for existing transfer (idempotency)
        if (self.getTransfer(t.id)) |existing| {
            return createTransferExists(t, existing);
        }

        // Parse flags
        const is_pending = t.flags.pending;
        const is_post_pending = t.flags.post_pending_transfer;
        const is_void_pending = t.flags.void_pending_transfer;

        // Check mutually exclusive flags
        const two_phase_count: u8 = @as(u8, @intFromBool(is_pending)) +
            @as(u8, @intFromBool(is_post_pending)) +
            @as(u8, @intFromBool(is_void_pending));
        if (two_phase_count > 1) {
            return .flags_are_mutually_exclusive;
        }

        // Validate account IDs
        if (t.debit_account_id == 0) return .debit_account_id_must_not_be_zero;
        if (t.debit_account_id == math.maxInt(u128)) return .debit_account_id_must_not_be_int_max;
        if (t.credit_account_id == 0) return .credit_account_id_must_not_be_zero;
        if (t.credit_account_id == math.maxInt(u128)) return .credit_account_id_must_not_be_int_max;
        if (t.debit_account_id == t.credit_account_id) return .accounts_must_be_different;

        // Validate pending_id constraints
        if (is_post_pending or is_void_pending) {
            if (t.pending_id == 0) return .pending_id_must_not_be_zero;
            if (t.pending_id == math.maxInt(u128)) return .pending_id_must_not_be_int_max;
            if (t.pending_id == t.id) return .pending_id_must_be_different;
        } else {
            if (t.pending_id != 0) return .pending_id_must_be_zero;
        }

        // Validate timeout constraints
        if (!is_pending and t.timeout != 0) {
            return .timeout_reserved_for_pending_transfer;
        }

        // Validate ledger and code
        if (t.ledger == 0) return .ledger_must_not_be_zero;
        if (t.code == 0) return .code_must_not_be_zero;

        // Get accounts
        const debit_account = self.getAccountMut(t.debit_account_id) orelse return .debit_account_not_found;
        const credit_account = self.getAccountMut(t.credit_account_id) orelse return .credit_account_not_found;

        // Check accounts are on same ledger
        if (debit_account.ledger != credit_account.ledger) {
            return .accounts_must_have_the_same_ledger;
        }

        // Check transfer ledger matches account ledgers
        if (t.ledger != debit_account.ledger) {
            return .transfer_must_have_the_same_ledger_as_accounts;
        }

        // Check accounts are not closed
        if (debit_account.flags.closed) return .debit_account_already_closed;
        if (credit_account.flags.closed) return .credit_account_already_closed;

        // Handle two-phase transfer operations
        if (is_post_pending or is_void_pending) {
            return self.handleTwoPhaseCompletion(timestamp, t, debit_account, credit_account);
        }

        // Get the actual transfer amount (may be adjusted for balancing)
        var amount = t.amount;

        // Check balance constraints and potentially adjust amount for balancing transfers
        if (debit_account.flags.debits_must_not_exceed_credits) {
            const available = safeSubtract(debit_account.credits_posted, debit_account.debits_posted + debit_account.debits_pending);
            if (amount > available) {
                if (t.flags.balancing_debit) {
                    if (available == 0) return .exceeds_credits;
                    amount = available;
                } else {
                    return .exceeds_credits;
                }
            }
        }

        if (credit_account.flags.credits_must_not_exceed_debits) {
            const available = safeSubtract(credit_account.debits_posted, credit_account.credits_posted + credit_account.credits_pending);
            if (amount > available) {
                if (t.flags.balancing_credit) {
                    if (available == 0) return .exceeds_debits;
                    amount = @min(amount, available);
                } else {
                    return .exceeds_debits;
                }
            }
        }

        // Check for overflow
        if (is_pending) {
            if (addOverflows(debit_account.debits_pending, amount)) return .overflows_debits_pending;
            if (addOverflows(credit_account.credits_pending, amount)) return .overflows_credits_pending;
        } else {
            if (addOverflows(debit_account.debits_posted, amount)) return .overflows_debits_posted;
            if (addOverflows(credit_account.credits_posted, amount)) return .overflows_credits_posted;
        }

        // Check capacity
        if (self.transfer_count >= MAX_TRANSFERS) {
            return .reserved_flag;
        }
        if (is_pending and self.pending_transfer_count >= MAX_PENDING_TRANSFERS) {
            return .reserved_flag;
        }

        // Apply the transfer
        if (is_pending) {
            debit_account.debits_pending += amount;
            credit_account.credits_pending += amount;

            // Track pending transfer
            const expires_at: u64 = if (t.timeout > 0) timestamp + @as(u64, t.timeout) * std.time.ns_per_s else 0;
            self.pending_transfers[self.pending_transfer_count] = PendingTransferInfo{
                .id = t.id,
                .original_amount = amount,
                .amount_posted = 0,
                .expires_at = expires_at,
                .state = PendingTransferInfo.STATE_ACTIVE,
            };
            self.pending_transfer_count += 1;
        } else {
            debit_account.debits_posted += amount;
            credit_account.credits_posted += amount;
        }

        // Store the transfer
        self.transfers[self.transfer_count] = Transfer{
            .id = t.id,
            .debit_account_id = t.debit_account_id,
            .credit_account_id = t.credit_account_id,
            .amount = amount,
            .pending_id = t.pending_id,
            .user_data_128 = t.user_data_128,
            .user_data_64 = t.user_data_64,
            .user_data_32 = t.user_data_32,
            .timeout = t.timeout,
            .ledger = t.ledger,
            .code = t.code,
            .flags = t.flags,
            .timestamp = timestamp,
        };
        self.transfer_count += 1;
        self.commit_timestamp = timestamp;

        return .ok;
    }

    /// Handle post_pending or void_pending transfer
    fn handleTwoPhaseCompletion(
        self: *Self,
        timestamp: u64,
        t: *const Transfer,
        debit_account: *Account,
        credit_account: *Account,
    ) CreateTransferResult {
        const is_post_pending = t.flags.post_pending_transfer;

        // Find the pending transfer
        const pending_info = self.getPendingTransfer(t.pending_id) orelse return .pending_transfer_not_found;

        // Check pending transfer is still active
        if (pending_info.state != PendingTransferInfo.STATE_ACTIVE) {
            if (pending_info.state == PendingTransferInfo.STATE_POSTED) return .pending_transfer_already_posted;
            if (pending_info.state == PendingTransferInfo.STATE_VOIDED) return .pending_transfer_already_voided;
            if (pending_info.state == PendingTransferInfo.STATE_EXPIRED) return .pending_transfer_expired;
            return .pending_transfer_not_pending;
        }

        // Check expiration
        if (pending_info.expires_at > 0 and timestamp >= pending_info.expires_at) {
            return .pending_transfer_expired;
        }

        // Get the original pending transfer
        const pending_transfer = self.getTransfer(t.pending_id) orelse return .pending_transfer_not_found;

        // Validate matching fields
        if (pending_transfer.debit_account_id != t.debit_account_id) return .pending_transfer_has_different_debit_account_id;
        if (pending_transfer.credit_account_id != t.credit_account_id) return .pending_transfer_has_different_credit_account_id;
        if (pending_transfer.ledger != t.ledger) return .pending_transfer_has_different_ledger;
        if (pending_transfer.code != t.code) return .pending_transfer_has_different_code;

        // Calculate amount
        var amount = t.amount;
        const remaining_amount = pending_info.original_amount - pending_info.amount_posted;

        if (is_post_pending) {
            // For post with zero amount, use remaining pending amount
            if (amount == 0) {
                amount = remaining_amount;
            }
            // Check amount doesn't exceed remaining
            if (amount > remaining_amount) {
                return .exceeds_pending_transfer_amount;
            }

            // Move from pending to posted
            debit_account.debits_pending = safeSubtract(debit_account.debits_pending, amount);
            debit_account.debits_posted += amount;
            credit_account.credits_pending = safeSubtract(credit_account.credits_pending, amount);
            credit_account.credits_posted += amount;

            // Update pending info
            pending_info.amount_posted += amount;

            // Check if fully posted
            if (pending_info.amount_posted >= pending_info.original_amount) {
                pending_info.state = PendingTransferInfo.STATE_POSTED;
            }
        } else {
            // Void: release remaining pending amounts
            const void_amount = remaining_amount;

            debit_account.debits_pending = safeSubtract(debit_account.debits_pending, void_amount);
            credit_account.credits_pending = safeSubtract(credit_account.credits_pending, void_amount);

            pending_info.state = PendingTransferInfo.STATE_VOIDED;
            amount = void_amount;
        }

        // Check capacity
        if (self.transfer_count >= MAX_TRANSFERS) {
            return .reserved_flag;
        }

        // Store the completion transfer
        self.transfers[self.transfer_count] = Transfer{
            .id = t.id,
            .debit_account_id = t.debit_account_id,
            .credit_account_id = t.credit_account_id,
            .amount = amount,
            .pending_id = t.pending_id,
            .user_data_128 = t.user_data_128,
            .user_data_64 = t.user_data_64,
            .user_data_32 = t.user_data_32,
            .timeout = t.timeout,
            .ledger = t.ledger,
            .code = t.code,
            .flags = t.flags,
            .timestamp = timestamp,
        };
        self.transfer_count += 1;
        self.commit_timestamp = timestamp;

        return .ok;
    }

    /// Check if a transfer already exists and return appropriate error
    fn createTransferExists(t: *const Transfer, e: *const Transfer) CreateTransferResult {
        if (@as(u16, @bitCast(t.flags)) != @as(u16, @bitCast(e.flags))) {
            return .exists_with_different_flags;
        }
        if (t.debit_account_id != e.debit_account_id) return .exists_with_different_debit_account_id;
        if (t.credit_account_id != e.credit_account_id) return .exists_with_different_credit_account_id;
        if (t.amount != e.amount) return .exists_with_different_amount;
        if (t.pending_id != e.pending_id) return .exists_with_different_pending_id;
        if (t.user_data_128 != e.user_data_128) return .exists_with_different_user_data_128;
        if (t.user_data_64 != e.user_data_64) return .exists_with_different_user_data_64;
        if (t.user_data_32 != e.user_data_32) return .exists_with_different_user_data_32;
        if (t.timeout != e.timeout) return .exists_with_different_timeout;
        if (t.code != e.code) return .exists_with_different_code;
        return .exists;
    }
};

/// Helper: safe subtraction (returns 0 if would underflow)
fn safeSubtract(a: u128, b: u128) u128 {
    if (b > a) return 0;
    return a - b;
}

/// Helper: check if addition would overflow
fn addOverflows(a: u128, b: u128) bool {
    const result = @addWithOverflow(a, b);
    return result[1] != 0;
}

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

/// Create transfers batch
/// Returns 0 on success, negative on error
/// The results buffer is populated with CreateTransfersResult structs for any errors
export fn tb_create_transfers(
    transfers_ptr: [*]const u8,
    transfers_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    // Calculate number of transfers
    const transfer_size = @sizeOf(Transfer);
    if (transfers_len % transfer_size != 0) {
        return -2; // Invalid input size
    }
    const num_transfers = transfers_len / transfer_size;

    // Cast input to Transfer slice
    const transfers = @as([*]const Transfer, @ptrCast(@alignCast(transfers_ptr)))[0..num_transfers];

    // Cast output to results slice
    const result_size = @sizeOf(CreateTransfersResult);
    const max_results = results_len.* / result_size;
    const results = @as([*]CreateTransfersResult, @ptrCast(@alignCast(results_ptr)))[0..max_results];

    var results_count: u32 = 0;
    const base_timestamp = vfs_time_monotonic();

    for (transfers, 0..) |*transfer, i| {
        // Each transfer gets a unique timestamp
        const timestamp = base_timestamp + @as(u64, @intCast(i));
        const result = state_machine.createTransfer(timestamp, transfer);

        // Only record errors (ok results are not included in output)
        // Note: .exists is considered idempotent success, but we still don't report it
        if (result != .ok and result != .exists) {
            if (results_count < max_results) {
                results[results_count] = CreateTransfersResult{
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

/// Lookup transfers by ID
/// Returns 0 on success, negative on error
/// The results buffer is populated with Transfer structs for found transfers
export fn tb_lookup_transfers(
    ids_ptr: [*]const u128,
    ids_len: u32,
    results_ptr: [*]u8,
    results_len: *u32,
) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    const ids = ids_ptr[0..ids_len];

    // Cast output to Transfer slice
    const transfer_size = @sizeOf(Transfer);
    const max_results = results_len.* / transfer_size;
    const results = @as([*]Transfer, @ptrCast(@alignCast(results_ptr)))[0..max_results];

    var results_count: u32 = 0;

    for (ids) |id| {
        if (state_machine.getTransfer(id)) |transfer| {
            if (results_count < max_results) {
                results[results_count] = transfer.*;
                results_count += 1;
            }
        }
        // Transfers not found are simply omitted from results
    }

    results_len.* = results_count * transfer_size;
    return 0;
}

/// Get transfers for an account
/// Returns all transfers where the account is either the debit or credit account
/// Results are returned in timestamp order
export fn tb_get_account_transfers(
    account_id: u128,
    results_ptr: [*]u8,
    results_max: u32,
    results_len: *u32,
) callconv(.C) i32 {
    if (!state_machine.initialized) {
        return -1; // Not initialized
    }

    // Cast output to Transfer slice
    const transfer_size = @sizeOf(Transfer);
    const max_results = results_max / transfer_size;
    const results = @as([*]Transfer, @ptrCast(@alignCast(results_ptr)))[0..max_results];

    var results_count: u32 = 0;

    // Scan all transfers (they are already in timestamp order)
    var i: u32 = 0;
    while (i < state_machine.transfer_count) : (i += 1) {
        const transfer = &state_machine.transfers[i];

        // Check if account is involved in this transfer
        if (transfer.debit_account_id == account_id or transfer.credit_account_id == account_id) {
            if (results_count < max_results) {
                results[results_count] = transfer.*;
                results_count += 1;
            } else {
                break; // Results buffer full
            }
        }
    }

    results_len.* = results_count * transfer_size;
    return 0;
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

/// Get current transfer count (for debugging/testing)
export fn tb_transfer_count() callconv(.C) u32 {
    return state_machine.transfer_count;
}

/// Get current pending transfer count (for debugging/testing)
export fn tb_pending_transfer_count() callconv(.C) u32 {
    return state_machine.pending_transfer_count;
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

/// State header for serialization (version 2 with transfers)
const StateHeader = packed struct {
    magic: u32 = 0x54425354, // "TBST" - TigerBeetle State
    version: u32 = 2, // Version 2 includes transfers
    account_count: u32,
    transfer_count: u32,
    pending_transfer_count: u32,
    commit_timestamp: u64,
};

/// Calculate the size needed to serialize current state
export fn tb_state_size() callconv(.C) u32 {
    const header_size = @sizeOf(StateHeader);
    const accounts_size = state_machine.account_count * @sizeOf(Account);
    const transfers_size = state_machine.transfer_count * @sizeOf(Transfer);
    const pending_size = state_machine.pending_transfer_count * @sizeOf(PendingTransferInfo);
    return @as(u32, header_size + accounts_size + transfers_size + pending_size);
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

    var offset: u32 = 0;

    // Write header
    const header = StateHeader{
        .account_count = state_machine.account_count,
        .transfer_count = state_machine.transfer_count,
        .pending_transfer_count = state_machine.pending_transfer_count,
        .commit_timestamp = state_machine.commit_timestamp,
    };
    const header_bytes = @as([*]const u8, @ptrCast(&header));
    @memcpy(buf_ptr[0..@sizeOf(StateHeader)], header_bytes[0..@sizeOf(StateHeader)]);
    offset += @sizeOf(StateHeader);

    // Write accounts
    if (state_machine.account_count > 0) {
        const accounts_size = state_machine.account_count * @sizeOf(Account);
        const accounts_bytes = @as([*]const u8, @ptrCast(&state_machine.accounts));
        @memcpy(buf_ptr[offset .. offset + accounts_size], accounts_bytes[0..accounts_size]);
        offset += accounts_size;
    }

    // Write transfers
    if (state_machine.transfer_count > 0) {
        const transfers_size = state_machine.transfer_count * @sizeOf(Transfer);
        const transfers_bytes = @as([*]const u8, @ptrCast(&state_machine.transfers));
        @memcpy(buf_ptr[offset .. offset + transfers_size], transfers_bytes[0..transfers_size]);
        offset += transfers_size;
    }

    // Write pending transfer info
    if (state_machine.pending_transfer_count > 0) {
        const pending_size = state_machine.pending_transfer_count * @sizeOf(PendingTransferInfo);
        const pending_bytes = @as([*]const u8, @ptrCast(&state_machine.pending_transfers));
        @memcpy(buf_ptr[offset .. offset + pending_size], pending_bytes[0..pending_size]);
    }

    return 0;
}

/// Version 1 header (for backward compatibility)
const StateHeaderV1 = packed struct {
    magic: u32,
    version: u32,
    account_count: u32,
    commit_timestamp: u64,
    reserved: u64,
};

/// Load state from a buffer
/// Returns 0 on success, negative on error
/// Supports both version 1 (accounts only) and version 2 (accounts + transfers)
export fn tb_load_state(buf_ptr: [*]const u8, buf_len: u32) callconv(.C) i32 {
    // Need at least enough for magic + version
    if (buf_len < 8) {
        return -1; // Buffer too small for header
    }

    // Read magic and version first
    const magic = @as(*const u32, @ptrCast(@alignCast(buf_ptr))).*;
    const version = @as(*const u32, @ptrCast(@alignCast(buf_ptr + 4))).*;

    // Validate magic
    if (magic != 0x54425354) {
        return -2; // Invalid magic
    }

    // Handle version 1 (backward compatibility)
    if (version == 1) {
        return loadStateV1(buf_ptr, buf_len);
    }

    // Handle version 2
    if (version == 2) {
        return loadStateV2(buf_ptr, buf_len);
    }

    return -3; // Unsupported version
}

/// Load version 1 state (accounts only)
fn loadStateV1(buf_ptr: [*]const u8, buf_len: u32) i32 {
    const header_size = @sizeOf(StateHeaderV1);

    if (buf_len < header_size) {
        return -1; // Buffer too small for header
    }

    // Read header
    const header = @as(*const StateHeaderV1, @ptrCast(@alignCast(buf_ptr))).*;

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
    state_machine.transfer_count = 0;
    state_machine.pending_transfer_count = 0;
    state_machine.commit_timestamp = header.commit_timestamp;
    state_machine.initialized = true;

    return 0;
}

/// Load version 2 state (accounts + transfers + pending transfers)
fn loadStateV2(buf_ptr: [*]const u8, buf_len: u32) i32 {
    const header_size = @sizeOf(StateHeader);

    if (buf_len < header_size) {
        return -1; // Buffer too small for header
    }

    // Read header
    const header = @as(*const StateHeader, @ptrCast(@alignCast(buf_ptr))).*;

    // Validate counts
    if (header.account_count > MAX_ACCOUNTS) {
        return -4; // Too many accounts
    }
    if (header.transfer_count > MAX_TRANSFERS) {
        return -6; // Too many transfers
    }
    if (header.pending_transfer_count > MAX_PENDING_TRANSFERS) {
        return -7; // Too many pending transfers
    }

    // Calculate expected size
    const accounts_size = header.account_count * @sizeOf(Account);
    const transfers_size = header.transfer_count * @sizeOf(Transfer);
    const pending_size = header.pending_transfer_count * @sizeOf(PendingTransferInfo);
    const expected_size = header_size + accounts_size + transfers_size + pending_size;

    if (buf_len < expected_size) {
        return -5; // Buffer too small
    }

    var offset: u32 = header_size;

    // Load accounts
    if (header.account_count > 0) {
        const accounts_bytes = @as([*]u8, @ptrCast(&state_machine.accounts));
        @memcpy(accounts_bytes[0..accounts_size], buf_ptr[offset .. offset + accounts_size]);
        offset += accounts_size;
    }

    // Load transfers
    if (header.transfer_count > 0) {
        const transfers_bytes = @as([*]u8, @ptrCast(&state_machine.transfers));
        @memcpy(transfers_bytes[0..transfers_size], buf_ptr[offset .. offset + transfers_size]);
        offset += transfers_size;
    }

    // Load pending transfer info
    if (header.pending_transfer_count > 0) {
        const pending_bytes = @as([*]u8, @ptrCast(&state_machine.pending_transfers));
        @memcpy(pending_bytes[0..pending_size], buf_ptr[offset .. offset + pending_size]);
    }

    state_machine.account_count = header.account_count;
    state_machine.transfer_count = header.transfer_count;
    state_machine.pending_transfer_count = header.pending_transfer_count;
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
