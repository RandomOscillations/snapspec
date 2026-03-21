#pragma once

#include "blockstore/base.hpp"
#include <fstream>
#include <mutex>
#include <vector>

namespace snapspec {

/// Copy-on-Write block store.
///
/// Snapshot creation: O(1) — create empty snapshot file + bitmap.
/// Write during snapshot (first write per block): 3 I/Os — read old, write old to snapshot, write new.
/// Write during snapshot (subsequent): 1 I/O — write directly to active.
/// Discard: O(1) — delete snapshot file (I/O cost was already paid during writes).
/// Commit: O(unmodified blocks) — copy unmodified blocks from active to snapshot to complete it.
class COWBlockStore : public BlockStore {
public:
    COWBlockStore() = default;
    ~COWBlockStore() override;

    void init(const std::string& base_path, size_t block_size, size_t total_blocks) override;
    void read(uint64_t block_id, uint8_t* buffer) override;
    void write(uint64_t block_id, const uint8_t* data,
               uint64_t timestamp, uint64_t dep_tag,
               WriteLogEntry::Role role, int partner) override;
    void create_snapshot(uint64_t snapshot_ts) override;
    size_t discard_snapshot() override;
    void commit_snapshot(const std::string& archive_path) override;
    std::vector<WriteLogEntry> get_write_log() const override;
    size_t get_delta_block_count() const override;
    bool is_snapshot_active() const override;
    size_t get_block_size() const override;
    size_t get_total_blocks() const override;

private:
    std::string active_path_;    // current writable image
    std::string snapshot_path_;  // receives COW'd blocks
    size_t block_size_ = 0;
    size_t total_blocks_ = 0;
    bool snapshot_active_ = false;
    uint64_t snapshot_ts_ = 0;

    std::fstream active_file_;
    std::fstream snapshot_file_;

    std::vector<bool> cow_bitmap_;  // tracks which blocks have already been COW'd
    size_t cow_count_ = 0;         // number of COW'd blocks
    std::vector<WriteLogEntry> write_log_;

    mutable std::mutex mu_;
};

}  // namespace snapspec
