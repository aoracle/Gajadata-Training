Compiling code and creating Jar
-------------------------------
Script: rebuild.sh
Code Dir: /scratch/hduser/code
The script rebuild the jar using WordCountSearch.java in the same dir.

Input data processing
---------------------
Script: input.sh
Input data dir: /scratch/hduser/input
DFS Input data dir: /scratch/hduser/dfsinput
The script removes dfsinput dir and copy the input dir files to dfsinput.

Running Hadoop Job
------------------
Script: hadoop.sh
Input DFS: /scratch/hduser/dfsinput
Output DFS: /scratch/hduser/dfsoutput
This script does the following
-Removes the outpt dfs dir
-Executes the hadoop job
-Prints the content the output file

Examples:
1. To get count of all the words
sh hadoop.sh
2. To get count of a specific word
sh hadoop.sh hello

Copy dfs resultfile to output folder
------------------------------------
Script: output.sh
Result file: /scratch/hduser/dfsoutput/part-r-00000
Local Output dir: /scratch/hduser/output
