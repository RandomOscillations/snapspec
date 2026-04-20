#include "blockstore/row.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace snapspec {

ROWBlockStore::~ROWBlockStore() {
    if (base_file_.is_open()) base_file_.close();
    if (delta_file_.is_open()) delta_file_.close();
}

void ROWBlockStore::init(const std::string& base_path, size_t block_size, size_t total_blocks) {
    base_path_ = base_path;
    delta_path_ = base_path + ".delta";
    block_size_ = block_size;
    total_blocks_ = total_blocks;

    // Create or open the base image file
    if (!std::filesystem::exists(base_path_)) {
        // Create and zero-fill the base image
        std::ofstream create(base_path_, std::ios::binary);
        std::vector<uint8_t> zeros(block_size_, 0);
        for (size_t i = 0; i < total_blocks_; ++i) {
            create.write(reinterpret_cast<const char*>(zeros.data()), block_size_);
        }
        create.close();
    }

    base_file_.open(base_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!base_file_.is_open()) {
        throw std::runtime_error("Failed to open base image: " + base_path_);
    }

    snapshot_active_ = false;
    bitmap_.clear();
    modified_blocks_.clear();
    write_log_.clear();
}

void ROWBlockStore::read(uint64_t block_id, uint8_t* buffer) {
    if (block_id >= total_blocks_) {
        throw std::out_of_range("Block ID out of range: " + std::to_string(block_id));
    }

    std::lock_guard<std::mutex> lock(mu_);

    if (snapshot_active_ && bitmap_[block_id]) {
        // Read from delta — this block was modified after snapshot
        delta_file_.seekg(static_cast<std::streamoff>(block_id * block_size_));
        delta_file_.read(reinterpret_cast<char*>(buffer), block_size_);
    } else {
        // Read from base
        base_file_.seekg(static_cast<std::streamoff>(block_id * block_size_));
        base_file_.read(reinterpret_cast<char*>(buffer), block_size_);
    }
}

void ROWBlockStore::write(uint64_t block_id, const uint8_t* data,
                          uint64_t timestamp, uint64_t dep_tag,
                          WriteLogEntry::Role role, int partner) {
    if (block_id >= total_blocks_) {
        throw std::out_of_range("Block ID out of range: " + std::to_string(block_id));
    }

    std::lock_guard<std::mutex> lock(mu_);

    if (snapshot_active_) {
        // Write to delta — base is frozen (IS the snapshot)
        delta_file_.seekp(static_cast<std::streamoff>(block_id * block_size_));
        delta_file_.write(reinterpret_cast<const char*>(data), block_size_);
        delta_file_.flush();

        if (!bitmap_[block_id]) {
            bitmap_[block_id] = true;
            modified_blocks_.push_back(block_id);
        }

        // Log writes that occurred after the snapshot boundary.
        if (timestamp > snapshot_ts_) {
            write_log_.push_back({block_id, timestamp, dep_tag, role, partner});
        }
    } else {
        // No snapshot — write directly to base
        base_file_.seekp(static_cast<std::streamoff>(block_id * block_size_));
        base_file_.write(reinterpret_cast<const char*>(data), block_size_);
        base_file_.flush();
    }
}

void ROWBlockStore::create_snapshot(uint64_t snapshot_ts) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(!snapshot_active_ && "Cannot create snapshot: one is already active");

    snapshot_ts_ = snapshot_ts;

    // Create empty delta file (same size as base for direct block addressing)
    {
        std::ofstream create(delta_path_, std::ios::binary);
        // Seek to end to set file size, or pre-allocate
        create.seekp(static_cast<std::streamoff>(total_blocks_ * block_size_ - 1));
        create.put(0);
        create.close();
    }

    delta_file_.open(delta_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!delta_file_.is_open()) {
        throw std::runtime_error("Failed to open delta file: " + delta_path_);
    }

    // Zero the bitmap — O(total_blocks) in metadata, but the bitmap is tiny
    // (32KB for a 1GB image with 4KB blocks)
    bitmap_.assign(total_blocks_, false);
    modified_blocks_.clear();
    write_log_.clear();

    snapshot_active_ = true;
    // Base file is now the snapshot — it's immutable until discard/commit
}

size_t ROWBlockStore::discard_snapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    assert(snapshot_active_ && "Cannot discard: no active snapshot");

    size_t delta_blocks = modified_blocks_.size();

    // Merge delta back into base — only iterate modified blocks
    std::vector<uint8_t> buf(block_size_);
    for (uint64_t bid : modified_blocks_) {
        delta_file_.seekg(static_cast<std::streamoff>(bid * block_size_));
        delta_file_.read(reinterpret_cast<char*>(buf.data()), block_size_);

        base_file_.seekp(static_cast<std::streamoff>(bid * block_size_));
        base_file_.write(reinterpret_cast<const char*>(buf.data()), block_size_);
    }
    base_file_.flush();

    // Cleanup
    delta_file_.close();
    std::filesystem::remove(delta_path_);
    bitmap_.clear();
    modified_blocks_.clear();
    write_log_.clear();
    snapshot_active_ = false;

    return delta_blocks;
}

void ROWBlockStore::commit_snapshot(const std::string& archive_path) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(snapshot_active_ && "Cannot commit: no active snapshot");

    std::filesystem::create_directories(
        std::filesystem::path(archive_path).parent_path());

    // Strategy C (In-place commit):
    // 1. Copy base to archive — the base IS the snapshot, so this archives it
    std::filesystem::copy_file(base_path_, archive_path,
                               std::filesystem::copy_options::overwrite_existing);

    // 2. Merge delta back into base (same as discard)
    std::vector<uint8_t> buf(block_size_);
    for (uint64_t bid : modified_blocks_) {
        delta_file_.seekg(static_cast<std::streamoff>(bid * block_size_));
        delta_file_.read(reinterpret_cast<char*>(buf.data()), block_size_);

        base_file_.seekp(static_cast<std::streamoff>(bid * block_size_));
        base_file_.write(reinterpret_cast<const char*>(buf.data()), block_size_);
    }
    base_file_.flush();

    // 3. Cleanup
    delta_file_.close();
    std::filesystem::remove(delta_path_);
    bitmap_.clear();
    modified_blocks_.clear();
    write_log_.clear();
    snapshot_active_ = false;
}

std::vector<WriteLogEntry> ROWBlockStore::get_write_log() const {
    std::lock_guard<std::mutex> lock(mu_);
    return write_log_;
}

size_t ROWBlockStore::get_delta_block_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return modified_blocks_.size();
}

bool ROWBlockStore::is_snapshot_active() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snapshot_active_;
}

size_t ROWBlockStore::get_block_size() const { return block_size_; }
size_t ROWBlockStore::get_total_blocks() const { return total_blocks_; }

}  // namespace snapspec
