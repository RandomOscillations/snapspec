#include <gtest/gtest.h>
#include "blockstore/row.hpp"
#include <cstring>
#include <filesystem>
#include <random>
#include <vector>

namespace fs = std::filesystem;

class ROWBlockStoreTest : public ::testing::Test {
protected:
    static constexpr size_t BLOCK_SIZE = 4096;
    static constexpr size_t TOTAL_BLOCKS = 256;  // 1MB test image

    snapspec::ROWBlockStore store;
    std::string test_dir;
    std::string base_path;

    void SetUp() override {
        test_dir = fs::temp_directory_path() / ("row_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()));
        fs::create_directories(test_dir);
        base_path = test_dir + "/base.img";
        store.init(base_path, BLOCK_SIZE, TOTAL_BLOCKS);
    }

    void TearDown() override {
        fs::remove_all(test_dir);
    }

    std::vector<uint8_t> make_block(uint8_t fill) {
        return std::vector<uint8_t>(BLOCK_SIZE, fill);
    }
};

// Test 1: Write blocks 0-99, read back, verify
TEST_F(ROWBlockStoreTest, BasicWriteRead) {
    for (uint64_t i = 0; i < 100; ++i) {
        auto data = make_block(static_cast<uint8_t>(i));
        store.write(i, data.data());
    }

    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < 100; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i)) << "Block " << i;
        EXPECT_EQ(buf[BLOCK_SIZE - 1], static_cast<uint8_t>(i)) << "Block " << i;
    }
}

// Test 2: Snapshot + writes go to delta, reads route correctly
TEST_F(ROWBlockStoreTest, SnapshotReadRouting) {
    // Write blocks 0-49 to base
    for (uint64_t i = 0; i < 50; ++i) {
        auto data = make_block(static_cast<uint8_t>(i));
        store.write(i, data.data());
    }

    store.create_snapshot(100);
    EXPECT_TRUE(store.is_snapshot_active());

    // Write blocks 50-149 to delta (some overlap with base blocks)
    for (uint64_t i = 50; i < 150; ++i) {
        auto data = make_block(static_cast<uint8_t>(i));
        store.write(i, data.data());
    }

    // Read all — blocks 0-49 from base, 50-149 from delta
    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < 150; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i)) << "Block " << i;
    }

    EXPECT_EQ(store.get_delta_block_count(), 100u);
}

// Test 3: Discard merges delta back, data intact
TEST_F(ROWBlockStoreTest, DiscardMergesCorrectly) {
    for (uint64_t i = 0; i < 50; ++i) {
        auto data = make_block(static_cast<uint8_t>(i));
        store.write(i, data.data());
    }

    store.create_snapshot(100);

    // Overwrite some base blocks + write new blocks in delta
    for (uint64_t i = 25; i < 75; ++i) {
        auto data = make_block(static_cast<uint8_t>(i + 100));
        store.write(i, data.data());
    }

    size_t delta_blocks = store.discard_snapshot();
    EXPECT_EQ(delta_blocks, 50u);
    EXPECT_FALSE(store.is_snapshot_active());

    // After discard, all data should be accessible from base
    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < 25; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i)) << "Block " << i;
    }
    for (uint64_t i = 25; i < 75; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i + 100)) << "Block " << i;
    }
}

// Test 4: Commit archives snapshot correctly
TEST_F(ROWBlockStoreTest, CommitArchives) {
    for (uint64_t i = 0; i < 50; ++i) {
        auto data = make_block(static_cast<uint8_t>(i));
        store.write(i, data.data());
    }

    store.create_snapshot(100);

    // Write new data to delta
    for (uint64_t i = 0; i < 50; ++i) {
        auto data = make_block(static_cast<uint8_t>(i + 200));
        store.write(i, data.data());
    }

    std::string archive_path = test_dir + "/snapshot_archive.img";
    store.commit_snapshot(archive_path);
    EXPECT_FALSE(store.is_snapshot_active());
    EXPECT_TRUE(fs::exists(archive_path));

    // Archive should contain the OLD data (base at time of snapshot)
    // Current base should contain the NEW data (merged from delta)
    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < 50; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], static_cast<uint8_t>(i + 200)) << "Block " << i << " in active";
    }
}

// Test 5: Stress test — random writes, discard, verify integrity
TEST_F(ROWBlockStoreTest, StressDiscard) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint64_t> block_dist(0, TOTAL_BLOCKS - 1);

    // Write known initial state
    for (uint64_t i = 0; i < TOTAL_BLOCKS; ++i) {
        auto data = make_block(static_cast<uint8_t>(i & 0xFF));
        store.write(i, data.data());
    }

    // Track expected state
    std::vector<uint8_t> expected(TOTAL_BLOCKS);
    for (size_t i = 0; i < TOTAL_BLOCKS; ++i) {
        expected[i] = static_cast<uint8_t>(i & 0xFF);
    }

    store.create_snapshot(1000);

    // 10000 random writes during snapshot
    for (int w = 0; w < 10000; ++w) {
        uint64_t bid = block_dist(rng);
        uint8_t val = static_cast<uint8_t>((w + 1) & 0xFF);
        auto data = make_block(val);
        store.write(bid, data.data());
        expected[bid] = val;
    }

    store.discard_snapshot();

    // Verify all data matches expected
    std::vector<uint8_t> buf(BLOCK_SIZE);
    for (uint64_t i = 0; i < TOTAL_BLOCKS; ++i) {
        store.read(i, buf.data());
        EXPECT_EQ(buf[0], expected[i]) << "Block " << i;
    }
}

// Test: Write log records entries correctly
TEST_F(ROWBlockStoreTest, WriteLogEntries) {
    store.create_snapshot(100);

    auto data = make_block(42);
    store.write(10, data.data(), 50, 1001, snapspec::WriteLogEntry::Role::CAUSE, 2);
    store.write(20, data.data(), 75, 1001, snapspec::WriteLogEntry::Role::EFFECT, 1);

    // Write with timestamp > snapshot_ts should NOT be logged
    store.write(30, data.data(), 200, 1002, snapspec::WriteLogEntry::Role::CAUSE, 3);

    auto log = store.get_write_log();
    EXPECT_EQ(log.size(), 2u);
    EXPECT_EQ(log[0].dependency_tag, 1001u);
    EXPECT_EQ(log[0].role, snapspec::WriteLogEntry::Role::CAUSE);
    EXPECT_EQ(log[1].role, snapspec::WriteLogEntry::Role::EFFECT);

    store.discard_snapshot();
}

// Test: Cannot create snapshot while one is active
TEST_F(ROWBlockStoreTest, DoubleSnapshotAsserts) {
    store.create_snapshot(100);
    EXPECT_DEATH(store.create_snapshot(200), "");
    store.discard_snapshot();
}
