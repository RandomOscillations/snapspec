# C++ Tests

Author: Adithya Srinivasan

This folder contains Google Test coverage for the C++ block stores.

## Files

- `row_test.cpp`: Redirect-on-Write behavior and post-snapshot write-log checks.
- `cow_test.cpp`: Copy-on-Write snapshot behavior.
- `fullcopy_test.cpp`: Full-copy snapshot behavior.

Run with:

```bash
ctest --test-dir build
```
