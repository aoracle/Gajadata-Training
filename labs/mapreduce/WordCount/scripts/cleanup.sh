#!/bin/bash

. ./props.sh

echo Input Dir: ${INPUTDIR}
echo Output Dir: ${OUTPUTDIR}
echo DFS Input Dir: ${DFSINPUT}
echo DFS Output Dir: ${DFSOUTPUT}

echo ---------------------

echo Deleting DFS input dir: ${DFSINPUT}
hadoop dfs -rmr ${DFSINPUT}
echo Done
echo ---------------------

echo Deleting DFS output dir: ${DFSOUTPUT}
hadoop dfs -rmr ${DFSOUTPUT}
echo Done
echo ---------------------

echo Cleaning input dir: ${INPUTDIR}
rm ${INPUTDIR}/*
echo Done

echo ---------------------

echo Cleaning output dir:${OUTPUTDIR}
rm ${OUTPUTDIR}/*
echo Done
echo ---------------------

