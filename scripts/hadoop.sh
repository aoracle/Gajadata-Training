#!/bin/bash

export CLASSNAME=AirTraffic
export INPUTDIR=/home/hadoop/input
export DFSINPUT=/home/hadoop/dfs-data/air_traffic
export DFSOUTPUT=/home/hadoop/dfsoutput
export CODEDIR=/home/hadoop
export BUILDDIR=/home/hadoop/lib
export RESULTFILE=part-r-00000

echo Input Word: $1

echo ----------------------------
echo Remove the dfs output dir
hadoop dfs -ls ${DFSOUTPUT}
hadoop dfs -rmr ${DFSOUTPUT}
echo Removed the output dir
echo ----------------------------

echo Run Hadoop Job
hadoop jar ${BUILDDIR}/${CLASSNAME}.jar ${CLASSNAME} $1 ${DFSINPUT} ${DFSOUTPUT}
echo Done Running Hadoop Job
echo ----------------------------

echo Print Output
hadoop dfs -cat ${DFSOUTPUT}/${RESULTFILE}
echo Done
echo ----------------------------
