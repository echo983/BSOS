# Open questions

Not yet resolved as of this document's creation. Resolve into
`docs/DESIGN.md` as decisions land.

- Exact wire format for declaring `key` + `size` up front on write (HTTP
  header shape; gRPC message field(s), presumably alongside/replacing
  `PutRequest.total_size`).
