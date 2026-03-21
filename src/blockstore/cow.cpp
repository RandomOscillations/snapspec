#include "blockstore/cow.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace snapspec {

COWBlockStore::~COWBlockStore() {
    if (active_file_.is_open()) active_file_.close();
    if (snapshot_file_.is_open()) snapshot_file_.close();
}

void COWBlockStore::init(const std::string& base_path, size_t block_size, size_t total_blocks) {
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
    cow_bitmap_.clear();
    write_log_.clear();
    cow_count_ = 0;
}

void COWBlockStore::read(uint64_t block_id, uint8_t* buffer) {
    if (block_id >= total_blocks_) {
        throw std::out_of_range("Block ID out of range");
    }

    std::lock_guard<std::mutex> lock(mu_);

    // Always read from the active file — COW preserves old data in snapshot file
    active_file_.seekg(static_cast<std::streamoff>(block_id * block_size_));
    active_file_.read(reinterpret_cast<char*>(buffer), block_size_);
}

void COWBlockStore::write(uint64_t block_id, const uint8_t* data,
                          uint64_t timestamp, uint64_t dep_tag,
                          WriteLogEntry::Role role, int partner) {
    if (block_id >= total_blocks_) {
        throw std::out_of_range("Block ID out of range");
    }

    std::lock_guard<std::mutex> lock(mu_);

    if (snapshot_active_) {
        // COW: on first write to this block post-snapshot, copy old data to snapshot
        if (!cow_bitmap_[block_id]) {
            // I/O 1: Read old data from active
            std::vector<uint8_t> old_data(block_size_);
            active_file_.seekg(static_cast<std::streamoff>(block_id * block_size_));
            active_file_.read(reinterpret_cast<char*>(old_data.data()), block_size_);

            // I/O 2: Write old data to snapshot file (preserve it)
            snapshot_file_.seekp(static_cast<std::streamoff>(block_id * block_size_));
            snapshot_file_.write(reinterpret_cast<const char*>(old_data.data()), block_size_);
            snapshot_file_.flush();

            cow_bitmap_[block_id] = true;
            cow_count_++;
        }

        // I/O 3 (or I/O 1 if already COW'd): Write new data to active
        active_file_.seekp(static_cast<std::streamoff>(block_id * block_size_));
        active_file_.write(reinterpret_cast<const char*>(data), block_size_);
        active_file_.flush();

        // Log if within snapshot window
        if (timestamp <= snapshot_ts_ && timestamp > 0) {
            write_log_.push_back({block_id, timestamp, dep_tag, role, partner});
        }
    } else {
        // No snapshot — write directly to active
        active_file_.seekp(static_cast<std::streamoff>(block_id * block_size_));
        active_file_.write(reinterpret_cast<const char*>(data), block_size_);
        active_file_.flush();
    }
}

void COWBlockStore::create_snapshot(uint64_t snapshot_ts) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(!snapshot_active_ && "Cannot create snapshot: one is already active");

    snapshot_ts_ = snapshot_ts;

    // Create snapshot file (same size as active for direct block addressing)
    {
        std::ofstream create(snapshot_path_, std::ios::binary);
        create.seekp(static_cast<std::streamoff>(total_blocks_ * block_size_ - 1));
        create.put(0);
        create.close();
    }

    snapshot_file_.open(snapshot_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!snapshot_file_.is_open()) {
        throw std::runtime_error("Failed to open snapshot file: " + snapshot_path_);
    }

    cow_bitmap_.assign(total_blocks_, false);
    cow_count_ = 0;
    write_log_.clear();
    snapshot_active_ = true;
}

size_t COWBlockStore::discard_snapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    assert(snapshot_active_ && "Cannot discard: no active snapshot");

    size_t blocks_cowd = cow_count_;

    // Discard is cheap for COW — just delete the snapshot file
    // The I/O cost was already paid during writes (3 I/O per first-write)
    snapshot_file_.close();
    std::filesystem::remove(snapshot_path_);
    cow_bitmap_.clear();
    cow_count_ = 0;
    write_log_.clear();
    snapshot_active_ = false;

    return blocks_cowd;
}

void COWBlockStore::commit_snapshot(const std::string& archive_path) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(snapshot_active_ && "Cannot commit: no active snapshot");

    // Complete the snapshot: copy un-COW'd blocks from active to snapshot file
    std::vector<uint8_t> buf(block_size_);
    for (size_t i = 0; i < total_blocks_; ++i) {
        if (!cow_bitmap_[i]) {
            active_file_.seekg(static_cast<std::streamoff>(i * block_size_));
            active_file_.read(reinterpret_cast<char*>(buf.data()), block_size_);

            snapshot_file_.seekp(static_cast<std::streamoff>(i * block_size_));
            snapshot_file_.write(reinterpret_cast<const char*>(buf.data()), block_size_);
        }
    }
    snapshot_file_.flush();
    snapshot_file_.close();

    // Move completed snapshot to archive location
    std::filesystem::rename(snapshot_path_, archive_path);

    cow_bitmap_.clear();
    cow_count_ = 0;
    write_log_.clear();
    snapshot_active_ = false;
}

std::vector<WriteLogEntry> COWBlockStore::get_write_log() const {
    std::lock_guard<std::mutex> lock(mu_);
    return write_log_;
}

size_t COWBlockStore::get_delta_block_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return cow_count_;
}

bool COWBlockStore::is_snapshot_active() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snapshot_active_;
}

size_t COWBlockStore::get_block_size() const { return block_size_; }
size_t COWBlockStore::get_total_blocks() const { return total_blocks_; }

}  // namespace snapspec
