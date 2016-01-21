# Group20 - ASU CSE 512 DDS Project #

Rectangle Definition: BottomLeft, TopRight

## Executable JARs for Operations ##
* closestPair-0.1.jar
* convexHull-0.1.jar
* farthestPair-0.1.jar
* joinQuery-0.1.jar
* rangeQuery-0.1.jar
* union-0.1.jar

## Dependant JARs ##
* convexHull-0.1.jar 	-> This JAR has common utility functions used in other jars
* jts-1.13.jar 		-> This JAR has topology suite package, used for cascaded polygon algorithm

## Run commands for the operations ##

__Note__: --jars flag requires absolute path of the dependant JARs. Please substitute the JAR name with its absolute path before running the command.

* Union
```
./spark-submit \
	--class edu.asu.cse512.Union \
	--master <Spark Master IP>  \
	--jars  convexHull-0.1.jar,jts-1.13.jar \
	union-0.1.jar hdfs://<HDFS Master IP>/inputFile hdfs://<HDFS Master IP>/outpuFile

```

* Closest Pair
```
./spark-submit \
	--class edu.asu.cse512.ClosestPair \
	--master <Spark Master IP>  \
	--jars convexHull-0.1.jar \
	closestPair-0.1.jar hdfs://<HDFS Master IP>/inputFile hdfs://<HDFS Master IP>/outputFile

```

* Convex Hull
```
./spark-submit \
	--class edu.asu.cse512.convexHull \
	--master <Spark Master IP>  \
	convexHull-0.1.jar hdfs://<HDFS Master IP>/inputFile hdfs://<HDFS Master IP>/outputFile
```

* Farthest Point
```
./spark-submit \
	--class edu.asu.cse512.FarthestPair \
	--master <Spark Master IP>  \
	--jars convexHull-0.1.jar \
	farthestPair-0.1.jar hdfs://<HDFS Master IP>/inputFile hdfs://<HDFS Master IP>/outputFile

```

* Spatial Join
```
./spark-submit \
	--class edu.asu.cse512.Join \
	--master <Spark Master IP>  \
	--jars convexHull-0.1.jar \
	joinQuery-0.1.jar hdfs://<HDFS Master IP>/inputFile1 \
	hdfs://<HDFS Master IP>/inputFile2 hdfs://<HDFS Master IP>/outputFile rectangle
```

```
./spark-submit \
	--class edu.asu.cse512.Join \
	--master <Spark Master IP>  \
	--jars convexHull-0.1.jar \
	joinQuery-0.1.jar hdfs://<HDFS Master IP>/inputFile1 \
	hdfs://<HDFS Master IP>/inputFile2 hdfs://<HDFS Master IP>/outputFile point
```

* Spatial Range
```
./spark-submit \
	--class edu.asu.cse512.RangeQuery \
	--master <Spark Master IP>  \
	--jars convexHull-0.1.jar \
	rangeQuery-0.1.jar hdfs://<HDFS Master IP>/inputFile1 \
	hdfs://<HDFS Master IP>/inputFile2 hdfs://<HDFS Master IP>/outputFile

```



