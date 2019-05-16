#!/usr/bin/env bash

numactl --show
numactl --hardware
numactl --cpunodebind=1 --physcpubind=6,7,8,9,10,11 tarantool perf/tarantool-server.lua 2>&1

STATUS=
while [ ${#STATUS} -eq "0" ]; do
    STATUS="$(echo box.info.status | tarantoolctl connect /tmp/tarantool-server.sock | grep -e "- running")"
    echo "waiting load snapshot to tarantool..."
    sleep 5
done

