#!/bin/sh

cd test
./test-run.py -j 30 --conf memtx $(for i in $(seq 1 100); do echo replication/test_create_cluster.test.lua; done)
cd ..
echo ""

for log_file in $(grep -R Test\ failed test/var/log | cut -d: -f1); do
    log_name=$(basename $log_file)
    worker_name=${log_name%.log}

    echo -ne "\e[01;31m"
    echo -e "Worker ${worker_name} fails. Look at the following files:\n"
    echo -ne "\e[00m"
    echo "* test/var/$worker_name/test_create_cluster1.log"
    echo "* test/var/$worker_name/test_create_cluster2.log"
    echo "* test/var/$worker_name/test_create_cluster3.log"
    echo ""

    echo -ne "\e[01;33m"
    echo -e "Grepping for errors:\n"
    echo -ne "\e[00m"
    grep 'E>' test/var/$worker_name/test_create_cluster[123].log
    echo ""

    echo -ne "\e[01;33m"
    echo -e "Grepping for a cluster topology:\n"
    echo -ne "\e[00m"

    grep "instance uuid\\|assigned id" test/var/$worker_name/test_create_cluster[123].log
    echo ""
done
