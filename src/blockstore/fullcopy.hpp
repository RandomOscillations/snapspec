#pragma once

#include "blockstore/base.hpp"
#include <fstream>
#include <mutex>
#include <vector>

namespace snapspec {

/// Full-Copy block store.
///
/// Snapshot creation: O(n) — full copy of the entire base image. Intentionally expensive.
/// Write during snapshot: 1 I/O — writes go to active, snapshot is a frozen copy.
/// Discard: O(1) — delete the snapshot file.
/// Commit: O(1) — snapshot file is already complete, just move it to archive.
class FullCopyBlockStore : public BlockStore {
public:
    FullCopyBlockStore() = default;
    ~FullCopyBlockStore() override;

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
    std::string active_path_;
    std::string snapshot_path_;
    size_t block_size_ = 0;
    size_t total_blocks_ = 0;
    bool snapshot_active_ = false;
    uint64_t snapshot_ts_ = 0;

    std::fstream active_file_;

    std::vector<WriteLogEntry> write_log_;
    size_t writes_during_snapshot_ = 0;

    mutable std::mutex mu_;
};

}  // namespace snapspec
