#!/bin/bash

export INPUTDIR=/home/hadoop/data/air_traffic_jan_2012.csv 
export DFSINPUT=/home/hadoop/dfs-data/air_traffic

echo Input Dir: ${INPUTDIR}
echo DFS Input Dir: ${DFSINPUT}

echo ---------------------

echo Deleting DFS input dir data1
hadoop dfs -rmr ${DFSINPUT}
echo Done
echo ---------------------

echo Copy from local input dir to dfs input 
hadoop dfs -copyFromLocal ${INPUTDIR} ${DFSINPUT}
echo Done
echo ---------------------

echo List the dfs input dir
hadoop dfs -ls ${DFSINPUT}
echo Done
echo ---------------------
