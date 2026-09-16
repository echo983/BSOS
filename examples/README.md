# BSOS Client Examples

This directory provides working code examples and integration patterns for interacting with BSOS daemons.

| Example | Language | Description |
| :--- | :--- | :--- |
| [`examples/go/cdc_chunking/`](go/cdc_chunking/main.go) | Go | FastCDC content-defined chunking: `PutCDC`, manifest inspection, `GetAuto` transparent retrieval, and sparse range reads. |
| [`examples/go/basic/`](go/basic/main.go) | Go | Core client operations: connection, health check, Bonnie capacity, Put, Head, Get, and Range reads. |
| [`examples/go/file_streaming/`](go/file_streaming/main.go) | Go | Single-pass memory-speed file streaming using `PutFileWithJumpRetry`, `ComputeFileFID`, and `GetFile`. |
| [`examples/python/`](python/) | Python | Cross-language integration using Python gRPC stubs and `xxhash`. |
| [`examples/bash/`](bash/pipeline.sh) | Bash | Shell automation recipes: stdin/stdout streaming, tarball pipelines, and JSON scripting with `jq`. |
