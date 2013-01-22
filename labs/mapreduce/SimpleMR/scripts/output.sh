#!/bin/bash

. ./props.sh

echo Output Dir: ${OUTPUTDIR}
echo DFS Output Dir: ${DFSOUTPUT}

echo ---------------------
echo Remove the output file if it already exists
rm ${OUTPUTDIR}/${RESULTFILE}

echo ---------------------
echo Copy the dfs output to local dir
hadoop dfs -copyToLocal ${DFSOUTPUT}/${RESULTFILE} ${OUTPUTDIR}
echo Done
echo ---------------------

echo Output file content
cat ${OUTPUTDIR}/${RESULTFILE}
echo Done
echo ---------------------
