#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "blockstore/base.hpp"
#include "blockstore/row.hpp"
#include "blockstore/cow.hpp"
#include "blockstore/fullcopy.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_blockstore, m) {
    m.doc() = "SnapSpec block store backends (C++ with pybind11)";

    // WriteLogEntry
    py::class_<snapspec::WriteLogEntry> wle(m, "WriteLogEntry");
    wle.def(py::init<>())
       .def_readwrite("block_id", &snapspec::WriteLogEntry::block_id)
       .def_readwrite("timestamp", &snapspec::WriteLogEntry::timestamp)
       .def_readwrite("dependency_tag", &snapspec::WriteLogEntry::dependency_tag)
       .def_readwrite("role", &snapspec::WriteLogEntry::role)
       .def_readwrite("partner_node_id", &snapspec::WriteLogEntry::partner_node_id);

    py::enum_<snapspec::WriteLogEntry::Role>(wle, "Role")
        .value("CAUSE", snapspec::WriteLogEntry::Role::CAUSE)
        .value("EFFECT", snapspec::WriteLogEntry::Role::EFFECT)
        .value("NONE", snapspec::WriteLogEntry::Role::NONE)
        .export_values();

    // Abstract BlockStore (for type hints / isinstance checks)
    py::class_<snapspec::BlockStore>(m, "BlockStore");

    // ROWBlockStore
    py::class_<snapspec::ROWBlockStore, snapspec::BlockStore>(m, "ROWBlockStore")
        .def(py::init<>())
        .def("init", &snapspec::ROWBlockStore::init)
        .def("read", [](snapspec::ROWBlockStore& self, uint64_t block_id) {
            std::vector<uint8_t> buf(self.get_block_size());
            self.read(block_id, buf.data());
            return py::bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        })
        .def("write", [](snapspec::ROWBlockStore& self, uint64_t block_id, py::bytes data,
                          uint64_t ts, uint64_t dep_tag, snapspec::WriteLogEntry::Role role, int partner) {
            std::string s = data;
            self.write(block_id, reinterpret_cast<const uint8_t*>(s.data()), ts, dep_tag, role, partner);
        }, py::arg("block_id"), py::arg("data"),
           py::arg("timestamp") = 0, py::arg("dep_tag") = 0,
           py::arg("role") = snapspec::WriteLogEntry::Role::NONE, py::arg("partner") = -1)
        .def("create_snapshot", &snapspec::ROWBlockStore::create_snapshot)
        .def("discard_snapshot", &snapspec::ROWBlockStore::discard_snapshot)
        .def("commit_snapshot", &snapspec::ROWBlockStore::commit_snapshot)
        .def("get_write_log", &snapspec::ROWBlockStore::get_write_log)
        .def("get_delta_block_count", &snapspec::ROWBlockStore::get_delta_block_count)
        .def("is_snapshot_active", &snapspec::ROWBlockStore::is_snapshot_active)
        .def("get_block_size", &snapspec::ROWBlockStore::get_block_size)
        .def("get_total_blocks", &snapspec::ROWBlockStore::get_total_blocks);

    // COWBlockStore
    py::class_<snapspec::COWBlockStore, snapspec::BlockStore>(m, "COWBlockStore")
        .def(py::init<>())
        .def("init", &snapspec::COWBlockStore::init)
        .def("read", [](snapspec::COWBlockStore& self, uint64_t block_id) {
            std::vector<uint8_t> buf(self.get_block_size());
            self.read(block_id, buf.data());
            return py::bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        })
        .def("write", [](snapspec::COWBlockStore& self, uint64_t block_id, py::bytes data,
                          uint64_t ts, uint64_t dep_tag, snapspec::WriteLogEntry::Role role, int partner) {
            std::string s = data;
            self.write(block_id, reinterpret_cast<const uint8_t*>(s.data()), ts, dep_tag, role, partner);
        }, py::arg("block_id"), py::arg("data"),
           py::arg("timestamp") = 0, py::arg("dep_tag") = 0,
           py::arg("role") = snapspec::WriteLogEntry::Role::NONE, py::arg("partner") = -1)
        .def("create_snapshot", &snapspec::COWBlockStore::create_snapshot)
        .def("discard_snapshot", &snapspec::COWBlockStore::discard_snapshot)
        .def("commit_snapshot", &snapspec::COWBlockStore::commit_snapshot)
        .def("get_write_log", &snapspec::COWBlockStore::get_write_log)
        .def("get_delta_block_count", &snapspec::COWBlockStore::get_delta_block_count)
        .def("is_snapshot_active", &snapspec::COWBlockStore::is_snapshot_active)
        .def("get_block_size", &snapspec::COWBlockStore::get_block_size)
        .def("get_total_blocks", &snapspec::COWBlockStore::get_total_blocks);

    // FullCopyBlockStore
    py::class_<snapspec::FullCopyBlockStore, snapspec::BlockStore>(m, "FullCopyBlockStore")
        .def(py::init<>())
        .def("init", &snapspec::FullCopyBlockStore::init)
        .def("read", [](snapspec::FullCopyBlockStore& self, uint64_t block_id) {
            std::vector<uint8_t> buf(self.get_block_size());
            self.read(block_id, buf.data());
            return py::bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        })
        .def("write", [](snapspec::FullCopyBlockStore& self, uint64_t block_id, py::bytes data,
                          uint64_t ts, uint64_t dep_tag, snapspec::WriteLogEntry::Role role, int partner) {
            std::string s = data;
            self.write(block_id, reinterpret_cast<const uint8_t*>(s.data()), ts, dep_tag, role, partner);
        }, py::arg("block_id"), py::arg("data"),
           py::arg("timestamp") = 0, py::arg("dep_tag") = 0,
           py::arg("role") = snapspec::WriteLogEntry::Role::NONE, py::arg("partner") = -1)
        .def("create_snapshot", &snapspec::FullCopyBlockStore::create_snapshot)
        .def("discard_snapshot", &snapspec::FullCopyBlockStore::discard_snapshot)
        .def("commit_snapshot", &snapspec::FullCopyBlockStore::commit_snapshot)
        .def("get_write_log", &snapspec::FullCopyBlockStore::get_write_log)
        .def("get_delta_block_count", &snapspec::FullCopyBlockStore::get_delta_block_count)
        .def("is_snapshot_active", &snapspec::FullCopyBlockStore::is_snapshot_active)
        .def("get_block_size", &snapspec::FullCopyBlockStore::get_block_size)
        .def("get_total_blocks", &snapspec::FullCopyBlockStore::get_total_blocks);
}
