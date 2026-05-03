# Source Directory

Author: Adithya Srinivasan

`src/` contains the C++ storage layer and pybind11 bindings used by the Python
runtime.

## Contents

- `blockstore/`: shared block-store interface plus ROW, COW, and Full-Copy
  implementations.
- `bindings.cpp`: pybind11 module exposing the C++ stores to Python.
- `tests/`: Google Test coverage for the C++ stores.

## Build

```bash
cmake -B build -S .
cmake --build build
```

The build produces `_blockstore` for Python import and `blockstore_tests` for
C++ unit tests.

## Test

```bash
ctest --test-dir build
```
