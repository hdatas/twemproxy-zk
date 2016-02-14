#!/bin/bash

proxy_addr="hcd1-10g"
proxy_port=21000
stats_addr="hcd1"
stats_port=31000
zk="ceph1:2181,ceph2:2181,ceph4:2181"

exe="../src/nutcracker"
pool="testpool1"

instances=8

for (( i = 0; i < $instances; i++ )); do
  p1=$(($proxy_port + $i))
  p2=$(($stats_port + $i))

  $exe -x $proxy_addr -y $p1 -z $zk -a $stats_addr -s $p2 -i 4000 -l $pool &
done
