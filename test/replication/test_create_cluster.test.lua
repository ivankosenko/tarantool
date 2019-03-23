test_run = require('test_run').new()

SERVERS = { 'test_create_cluster1', 'test_create_cluster2', 'test_create_cluster3' }
test_run:create_cluster(SERVERS, "replication", {args="0.1"})
assert(test_run:wait_fullmesh(SERVERS) == nil)
