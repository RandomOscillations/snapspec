#include <gtest/gtest.h>
#include "blockstore/fullcopy.hpp"
#include <cstring>
#include <filesystem>
#include <random>
#include <vector>

namespace fs = std::filesystem;

class FullCopyBlockStoreTest : public ::testing::Test {
protected:
    static constexpr size_t BLOCK_SIZE = 4096;
    static constexpr size_t TOTAL_BLOCKS = 256;

    snapspec::FullCopyBlockStore store;
    std::string test_dir;
    std::string base_path;

    void SetUp() override {
        test_dir = fs::temp_directory_path() / ("fullcopy_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()));
        fs::create_directories(test_dir);
        base_path = test_dir + "/active.img";
        store.init(base_path, BLOCK_SIZE, TOTAL_BLOCKS);
    }

    void TearDown() override {
        fs::remove_all(test_dir);
    }

    std::vector<uint8_t> make_block(uint8_t fill) {
        return std::vector<uint8_t>(BLOCK_SIZE, fill);
    }
};

TEST_F(FullCopyBlockStoreTest, BasicWriteRead) {
    for (uint64_t i = 0; i < 100; ++i) {
        auto data = make_block(static_cast<uint8_t>(i));
        store.write(i, data.data());
    }

    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < 100; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i));
    }
}

TEST_F(FullCopyBlockStoreTest, SnapshotIsFullCopy) {
    for (uint64_t i = 0; i < TOTAL_BLOCKS; ++i) {
        auto data = make_block(static_cast<uint8_t>(i & 0xFF));
        store.write(i, data.data());
    }

    store.create_snapshot(100);

    // Writes after snapshot go to active only
    for (uint64_t i = 0; i < 50; ++i) {
        auto data = make_block(static_cast<uint8_t>(i + 100));
        store.write(i, data.data());
    }

    // Active should have new data
    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < 50; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i + 100));
    }

    store.discard_snapshot();
}

TEST_F(FullCopyBlockStoreTest, CommitMovesToArchive) {
    for (uint64_t i = 0; i < TOTAL_BLOCKS; ++i) {
        auto data = make_block(static_cast<uint8_t>(i & 0xFF));
        store.write(i, data.data());
    }

    store.create_snapshot(100);

    std::string archive_path = test_dir + "/snapshot_archive.img";
    store.commit_snapshot(archive_path);

    EXPECT_TRUE(fs::exists(archive_path));
    EXPECT_FALSE(store.is_snapshot_active());

    // Archive should be the size of the full image
    auto archive_size = fs::file_size(archive_path);
    EXPECT_EQ(archive_size, BLOCK_SIZE * TOTAL_BLOCKS);
}

TEST_F(FullCopyBlockStoreTest, DiscardDeletesSnapshot) {
    store.create_snapshot(100);
    store.discard_snapshot();
    EXPECT_FALSE(store.is_snapshot_active());
}

TEST_F(FullCopyBlockStoreTest, StressDiscard) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint64_t> block_dist(0, TOTAL_BLOCKS - 1);

    for (uint64_t i = 0; i < TOTAL_BLOCKS; ++i) {
        auto data = make_block(static_cast<uint8_t>(i & 0xFF));
        store.write(i, data.data());
    }

    std::vector<uint8_t> expected(TOTAL_BLOCKS);
    for (size_t i = 0; i < TOTAL_BLOCKS; ++i) {
        expected[i] = static_cast<uint8_t>(i & 0xFF);
    }

    store.create_snapshot(1000);

    for (int w = 0; w < 10000; ++w) {
        uint64_t bid = block_dist(rng);
        uint8_t val = static_cast<uint8_t>((w + 1) & 0xFF);
        auto data = make_block(val);
        store.write(bid, data.data());
        expected[bid] = val;
    }

    store.discard_snapshot();

    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < TOTAL_BLOCKS; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], expected[i]) << "Block " << i;
    }
}
