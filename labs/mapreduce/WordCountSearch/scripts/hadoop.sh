#!/bin/bash

. ./props.sh

echo CLASSNAME=$CLASSNAME
echo INPUTDIR=$INPUTDIR
echo DFSINPUT=$DFSINPUT
echo DFSOUTPUT=$DFSOUTPUT
echo CODEDIR=$CODEDIR
echo BUILDDIR=$BUILDDIR
echo RESULTFILE=$RESULTFILE

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
