#!/bin/bash
# Script to Deleting old log files
# By Anand Mahajan

#EXPECTED_ARGS=1
#if [ $# -ne $EXPECTED_ARGS ]
#then
#  echo "Script to delete old log files, specify how many olds to keep"
#  echo "Usage: `basename $0` <days>"
#  exit 2
#fi
day=`date`
olderthen=3
logdir="/home/hadoop/hd/logs"
#EDIT HERE
space_before=`du -mh $logdir | tail -1 | awk {'print $1'}`

#FINISH EDIT
 
echo  ". Deleting files older then $olderthen days " >> $logdir/logcleanup.txt
echo  ". On $day" >> $logdir/logcleanup.txt
echo  ". Deleting old files in $logdir ">> $logdir/logcleanup.txt

#find .  -name "*.log*" -mtime +10 -exec rm -rf {} \; >> $logdir/logcleanup.txt 

find $logdir -type f \( -name "*.xml" -o -name "*.log*" -o -name "*.out*" \) -mtime +10 -exec rm -rf {} \; >> $logdir/logcleanup.txt 

#EDIT HERE
space_after=`du -mh $logdir | tail -1 | awk {'print $1'}`
echo  ". Before had $space_before used, now $space_after used. " >> $logdir/logcleanup.txt
 



#delete_log_files()
#{
#while read dirname; do
#   find $dirname -type f -name "*.log" -mtime +3 -exec rm {} \ # do whatever you want here
#done < /path/to/list_of_dirnames
#}

