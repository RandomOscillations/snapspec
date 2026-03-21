#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace snapspec {

/// A single entry in the write log, recorded during a snapshot window.
struct WriteLogEntry {
    uint64_t block_id;
    uint64_t timestamp;
    uint64_t dependency_tag;
    enum class Role { CAUSE, EFFECT, NONE } role;
    int partner_node_id;  // -1 if local write (no dependency)
};

/// Abstract interface for all block store backends.
/// ROW, COW, and Full-Copy all implement this identically.
class BlockStore {
public:
    virtual ~BlockStore() = default;

    /// Initialize the block store with a base image file.
    /// @param base_path  Path to the base image file (created if not exists)
    /// @param block_size Size of each block in bytes (e.g., 4096)
    /// @param total_blocks Total number of blocks in the image
    virtual void init(const std::string& base_path, size_t block_size, size_t total_blocks) = 0;

    /// Read a single block.
    /// @param block_id Block index (0 to total_blocks-1)
    /// @param buffer   Output buffer, must be at least block_size bytes
    virtual void read(uint64_t block_id, uint8_t* buffer) = 0;

    /// Write a single block.
    /// @param block_id Block index
    /// @param data     Input buffer, must be exactly block_size bytes
    /// @param timestamp Logical timestamp from coordinator
    /// @param dep_tag  Dependency tag (0 if no dependency)
    /// @param role     Dependency role
    /// @param partner  Partner node ID (-1 if local)
    virtual void write(uint64_t block_id, const uint8_t* data,
                       uint64_t timestamp = 0, uint64_t dep_tag = 0,
                       WriteLogEntry::Role role = WriteLogEntry::Role::NONE,
                       int partner = -1) = 0;

    /// Create a snapshot. Asserts no snapshot is currently active.
    /// @param snapshot_ts Logical timestamp marking the snapshot boundary
    virtual void create_snapshot(uint64_t snapshot_ts) = 0;

    /// Discard (abort) the current snapshot. Merges delta back into base.
    /// @return Number of blocks that were in the delta (for metrics)
    virtual size_t discard_snapshot() = 0;

    /// Commit the current snapshot.
    /// @param archive_path Path to store the committed snapshot image
    virtual void commit_snapshot(const std::string& archive_path) = 0;

    /// Get the write log for the current snapshot window.
    virtual std::vector<WriteLogEntry> get_write_log() const = 0;

    /// Get the number of modified blocks in the current delta (bitmap popcount).
    virtual size_t get_delta_block_count() const = 0;

    /// Check if a snapshot is currently active.
    virtual bool is_snapshot_active() const = 0;

    /// Get block size.
    virtual size_t get_block_size() const = 0;

    /// Get total block count.
    virtual size_t get_total_blocks() const = 0;
};

}  // namespace snapspec
