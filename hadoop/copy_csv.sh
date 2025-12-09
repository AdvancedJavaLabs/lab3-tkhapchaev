#!/bin/bash

SRC="/input/7.csv"
DEST="/input/copied"
COUNT=200

hdfs dfs -mkdir -p $DEST

for i in $(seq -f "%04g" 1 $COUNT); do
    hdfs dfs -cp $SRC "$DEST/7_$i.csv"
done