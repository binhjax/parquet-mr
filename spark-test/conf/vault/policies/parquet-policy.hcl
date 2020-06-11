path "kv/data/*" {
  capabilities = [ "create", "read", "update", "delete" ]
}
path "kv/*" {
  capabilities = [ "create", "read", "update", "delete" ]
}
