#!/bin/bash

. ./props.sh

echo Input Dir: ${INPUTDIR}
echo DFS Input Dir: ${DFSINPUT}

echo ---------------------

echo Deleting DFS input dir: ${DFSINPUT}
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
