# cse512-15fall-project - Group 20
Project for CSD512 Distribute and Parallel Database, 15 Fall semester, Arizona State University.

## Running the operations ##

* Union
```
./spark-submit \
	--class edu.asu.cse512.Union \
	--master spark://192.168.42.201:7077  \
	--jars  convexHull-0.1.jar,/home/vageeshb/.m2/repository/com/vividsolutions/jts/1.13/jts-1.13.jar \
	union-0.1.jar hdfs://192.168.42.201:54310/data/UnionQueryTestData.csv hdfs://192.168.42.201:54310/output/union

```

* Closest Pair
```
./spark-submit \
	--class edu.asu.cse512.ClosestPair \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	closestPair-0.1.jar hdfs://192.168.42.201:54310/data/ClosestPairTestData.csv hdfs://192.168.42.201:54310/output/closest

```

* Convex Hull
```
./spark-submit \
	--class edu.asu.cse512.convexHull \
	--master spark://192.168.42.201:7077  \
	convexHull-0.1.jar hdfs://192.168.42.201:54310/data/ConvexHullTestData.csv hdfs://192.168.42.201:54310/output/convex
```

* Farthest Point
```
./spark-submit \
	--class edu.asu.cse512.FarthestPair \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	farthestPair-0.1.jar hdfs://192.168.42.201:54310/data/FarthestPairTestData.csv hdfs://192.168.42.201:54310/output/farthest

```

* Spatial Join
```
./spark-submit \
	--class edu.asu.cse512.Join \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	joinQuery-0.1.jar hdfs://192.168.42.201:54310/data/JoinQueryInput1.csv \
	hdfs://192.168.42.201:54310/data/JoinQueryInput2.csv hdfs://192.168.42.201:54310/output/joinRectangle rectangle
```

```
./spark-submit \
	--class edu.asu.cse512.Join \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	joinQuery-0.1.jar hdfs://192.168.42.201:54310/data/RangeQueryTestData.csv \
	hdfs://192.168.42.201:54310/data/JoinQueryRectangle.csv hdfs://192.168.42.201:54310/output/joinPoint point
```

* Spatial Range
```
./spark-submit \
	--class edu.asu.cse512.RangeQuery \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar \
	rangeQuery-0.1.jar hdfs://192.168.42.201:54310/data/RangeQueryTestData.csv \
	hdfs://192.168.42.201:54310/data/RangeQueryRectangle.csv hdfs://192.168.42.201:54310/output/range

```

* Spatial Aggregation
```
./spark-submit \
	--class edu.asu.cse512.aggregation \
	--master spark://192.168.42.201:7077  \
	--jars convexHull-0.1.jar,joinQuery-0.1.jar \
	spatialAggregation-0.1.jar hdfs://192.168.42.201:54310/data/AggregationInput1.csv \
	hdfs://192.168.42.201:54310/data/AggregationInput2.csv hdfs://192.168.42.201:54310/output/aggregation rectangle

```
