#!/bin/sh
# Author: Aurimas Repecka <aurimas.repecka AT gmail [DOT] com>
# A wrapper script to submit spark job with hdfs-es.sh script

bash hdfs-es.sh --basedir hdfs:///user/arepecka/ReplicaMonitoring \
                --fromdate 2016-09-21 \
                --todate 2016-09-21 \
                --esorigin test \
                #--logs error
                #--fname /home/aurimas/CERN/ReplicaMonitoring/v2/data/project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s


