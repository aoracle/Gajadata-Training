#!/bin/bash

echo --------------------------------
echo Start....

export JAVAFILE="AirTraffic"
export CODEDIR=/home/hadoop/AirTraffic/${JAVAFILE}
export BUILDDIR=/home/hadoop/lib

cd ${CODEDIR}
ls

echo --------------------------------
echo Cleanup start
cd ${BUILDDIR}
echo Removing files..
ls ${JAVAFILE}*.jar
rm ${JAVAFILE}*.jar
echo Clean up Done
echo --------------------------------

echo Compile Java file
cd ${CODEDIR}
javac -classpath /scratch/hduser/hadoop-1.0.3/hadoop-core-1.0.3.jar:/scratch/hduser/hadoop-1.0.3/hadoop-client-1.0.3.jar ${JAVAFILE}.java
echo Compile Done
echo --------------------------------

echo Creating jar
jar cvf ${BUILDDIR}/${JAVAFILE}.jar ${JAVAFILE}*.class
echo Jar Created
echo --------------------------------

echo Verifying Jar
jar tvf ${BUILDDIR}/${JAVAFILE}.jar
echo Jar Verified
echo --------------------------------

echo End...
echo --------------------------------
