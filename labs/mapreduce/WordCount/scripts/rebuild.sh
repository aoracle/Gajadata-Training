#!/bin/bash

. ./props.sh

echo --------------------------------
echo Start....

echo CLASSNAME=${CLASSNAME}
echo CODEDIR=${CODEDIR}
echo BUILDDIR=${BUILDDIR} 

cd ${CODEDIR}
ls
rm *.class

echo --------------------------------
echo Cleanup start
mkdir ${BUILDDIR}
cd ${BUILDDIR}
echo Removing files..
ls ${CLASSNAME}*.jar
rm ${CLASSNAME}*.jar
echo Clean up Done
echo --------------------------------

echo Compile Java file
cd ${CODEDIR}
javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.3.jar:${HADOOP_HOME}/hadoop-client-1.0.3.jar ${CLASSNAME}.java
echo Compile Done
echo --------------------------------

echo Creating jar
jar cvf ${BUILDDIR}/${CLASSNAME}.jar ${CLASSNAME}*.class
echo Jar Created
echo --------------------------------

echo Verifying Jar
jar tvf ${BUILDDIR}/${CLASSNAME}.jar
echo Jar Verified
echo --------------------------------

echo End...
echo --------------------------------
