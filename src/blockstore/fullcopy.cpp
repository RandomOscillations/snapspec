#include "blockstore/fullcopy.hpp"
#include <cassert>
#include <filesystem>
#include <stdexcept>

namespace snapspec {

FullCopyBlockStore::~FullCopyBlockStore() {
    if (active_file_.is_open()) active_file_.close();
}

void FullCopyBlockStore::init(const std::string& base_path, size_t block_size, size_t total_blocks) {
    active_path_ = base_path;
    snapshot_path_ = base_path + ".snapshot";
    block_size_ = block_size;
    total_blocks_ = total_blocks;

    if (!std::filesystem::exists(active_path_)) {
        std::ofstream create(active_path_, std::ios::binary);
        std::vector<uint8_t> zeros(block_size_, 0);
        for (size_t i = 0; i < total_blocks_; ++i) {
            create.write(reinterpret_cast<const char*>(zeros.data()), block_size_);
        }
        create.close();
    }

    active_file_.open(active_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!active_file_.is_open()) {
        throw std::runtime_error("Failed to open active image: " + active_path_);
    }

    snapshot_active_ = false;
    write_log_.clear();
    writes_during_snapshot_ = 0;
}

void FullCopyBlockStore::read(uint64_t block_id, uint8_t* buffer) {
    if (block_id >= total_blocks_) {
        throw std::out_of_range("Block ID out of range");
    }

    std::lock_guard<std::mutex> lock(mu_);

    active_file_.seekg(static_cast<std::streamoff>(block_id * block_size_));
    active_file_.read(reinterpret_cast<char*>(buffer), block_size_);
}

void FullCopyBlockStore::write(uint64_t block_id, const uint8_t* data,
                                uint64_t timestamp, uint64_t dep_tag,
                                WriteLogEntry::Role role, int partner) {
    if (block_id >= total_blocks_) {
        throw std::out_of_range("Block ID out of range");
    }

    std::lock_guard<std::mutex> lock(mu_);

    // Always write to active — snapshot is a separate complete copy
    active_file_.seekp(static_cast<std::streamoff>(block_id * block_size_));
    active_file_.write(reinterpret_cast<const char*>(data), block_size_);
    active_file_.flush();

    if (snapshot_active_) {
        writes_during_snapshot_++;

        // Log ALL writes during active snapshot.
        write_log_.push_back({block_id, timestamp, dep_tag, role, partner});
    }
}

void FullCopyBlockStore::create_snapshot(uint64_t snapshot_ts) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(!snapshot_active_ && "Cannot create snapshot: one is already active");

    snapshot_ts_ = snapshot_ts;

    // Full copy of the entire base image — O(n), intentionally expensive
    active_file_.flush();
    std::filesystem::copy_file(active_path_, snapshot_path_,
                               std::filesystem::copy_options::overwrite_existing);

    write_log_.clear();
    writes_during_snapshot_ = 0;
    snapshot_active_ = true;
}

size_t FullCopyBlockStore::discard_snapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    assert(snapshot_active_ && "Cannot discard: no active snapshot");

    size_t writes = writes_during_snapshot_;

    // Just delete the snapshot file
    std::filesystem::remove(snapshot_path_);
    write_log_.clear();
    writes_during_snapshot_ = 0;
    snapshot_active_ = false;

    return writes;
}

void FullCopyBlockStore::commit_snapshot(const std::string& archive_path) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(snapshot_active_ && "Cannot commit: no active snapshot");

    std::filesystem::create_directories(
        std::filesystem::path(archive_path).parent_path());

    // Snapshot file is already a complete copy — just move it to archive
    std::filesystem::rename(snapshot_path_, archive_path);

    write_log_.clear();
    writes_during_snapshot_ = 0;
    snapshot_active_ = false;
}

std::vector<WriteLogEntry> FullCopyBlockStore::get_write_log() const {
    std::lock_guard<std::mutex> lock(mu_);
    return write_log_;
}

size_t FullCopyBlockStore::get_delta_block_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return writes_during_snapshot_;
}

bool FullCopyBlockStore::is_snapshot_active() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snapshot_active_;
}

size_t FullCopyBlockStore::get_block_size() const { return block_size_; }
size_t FullCopyBlockStore::get_total_blocks() const { return total_blocks_; }

}  // namespace snapspec
