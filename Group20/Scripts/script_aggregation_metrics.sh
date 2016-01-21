#! /bin/bash

echo "Removing all output folders"
/home/vageeshb/hadoop-2.6.0/bin/hadoop fs -rm -r /output/*

echo -e "\nCreating log file in $1"
file="($1)_logFile.txt"
filePretty="($1)_logFilePretty.txt"
rm $file
rm $filePretty
touch $file
touch $filePretty

echo -e "\nStarting operation"
totalStart=`date +"%m/%d/%Y %H:%M"`

echo -e "\nAggregation"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.aggregation \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar,joinQuery-0.1.jar \
	spatialAggregation-0.1.jar hdfs://192.168.42.201:54310/data/JoinQueryInput1.csv hdfs://192.168.42.201:54310/data/JoinQueryInput2.csv \
	hdfs://192.168.42.201:54310/output/aggregation rectangle
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty
totalEnd=`date +"%m/%d/%Y %H:%M+1"`

echo -e "\nCompleted\n"
echo $totalStart
echo $totalEnd
