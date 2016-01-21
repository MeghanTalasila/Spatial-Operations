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

echo -e "\nStarting operations"
totalStart=`date +"%m/%d/%Y %H:%M"`

echo -e "\nUnion"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.Union \
	--master spark://192.168.42.201:7077  \
	--jars  convexHull-0.1.jar,jts.jar \
	union-0.1.jar hdfs://192.168.42.201:54310/data/RectanglesWithoutID.csv hdfs://192.168.42.201:54310/output/Union
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty


echo -e "\nClosest Pair"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.ClosestPair \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	closestPair-0.1.jar hdfs://192.168.42.201:54310/data/PointsWithoutID.csv hdfs://192.168.42.201:54310/output/closestPair
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty

echo -e "\nConvex Hull"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.convexHull \
	--master spark://192.168.42.201:7077  \
	convexHull-0.1.jar hdfs://192.168.42.201:54310/data/PointsWithoutID.csv hdfs://192.168.42.201:54310/output/convexHull
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty

echo -e "\nFarthest Pair"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.FarthestPair \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	farthestPair-0.1.jar hdfs://192.168.42.201:54310/data/PointsWithoutID.csv hdfs://192.168.42.201:54310/output/farthestPoint
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty

echo -e "\nSpatial Join"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.Join \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	joinQuery-0.1.jar hdfs://192.168.42.201:54310/data/Points.csv \
	hdfs://192.168.42.201:54310/data/Rectangles.csv hdfs://192.168.42.201:54310/output/spatialJoin point
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty

echo -e "\nSpatial Range"
start=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "Start Time: $start"
/home/vageeshb/spark-1.2.0/bin/spark-submit \
	--class edu.asu.cse512.RangeQuery \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	rangeQuery-0.1.jar hdfs://192.168.42.201:54310/data/Points.csv \
	hdfs://192.168.42.201:54310/data/RectanglesRange.csv hdfs://192.168.42.201:54310/output/Range
end=$(date +"%Y-%m-%d %H:%M:%S.%3N")
echo "End Time: $end"
echo -e "`date --date="$start" '+%s'`,`date --date="$end" '+%s'`" >> $file
echo "$start, $end" >> $filePretty																					 

totalEnd=`date +"%m/%d/%Y %H:%M+1"`

echo -e "\nCompleted\n"
echo $totalStart
echo $totalEnd
