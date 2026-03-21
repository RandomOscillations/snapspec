#pragma once

#include "blockstore/base.hpp"
#include <fstream>
#include <memory>
#include <mutex>
#include <vector>

namespace snapspec {

/// Redirect-on-Write block store.
///
/// Snapshot creation: O(1) — create empty delta file + zeroed bitmap.
/// Write during snapshot: 1 I/O — write to delta only.
/// Discard: O(delta) — merge modified blocks back to base.
/// Commit: O(n) — archive base, merge delta into base.
class ROWBlockStore : public BlockStore {
public:
    ROWBlockStore() = default;
    ~ROWBlockStore() override;

    void init(const std::string& base_path, size_t block_size, size_t total_blocks) override;
    void read(uint64_t block_id, uint8_t* buffer) override;
    void write(uint64_t block_id, const uint8_t* data,
               uint64_t timestamp = 0, uint64_t dep_tag = 0,
               WriteLogEntry::Role role = WriteLogEntry::Role::NONE,
               int partner = -1) override;
    void create_snapshot(uint64_t snapshot_ts) override;
    size_t discard_snapshot() override;
    void commit_snapshot(const std::string& archive_path) override;
    std::vector<WriteLogEntry> get_write_log() const override;
    size_t get_delta_block_count() const override;
    bool is_snapshot_active() const override;
    size_t get_block_size() const override;
    size_t get_total_blocks() const override;

private:
    std::string base_path_;
    std::string delta_path_;
    size_t block_size_ = 0;
    size_t total_blocks_ = 0;
    bool snapshot_active_ = false;
    uint64_t snapshot_ts_ = 0;

    std::fstream base_file_;
    std::fstream delta_file_;

    std::vector<bool> bitmap_;            // 0 = read from base, 1 = read from delta
    std::vector<uint64_t> modified_blocks_;  // sparse list of modified block IDs (for fast merge)
    std::vector<WriteLogEntry> write_log_;

    mutable std::mutex mu_;  // protects snapshot state against concurrent write/snapshot races
};

}  // namespace snapspec
