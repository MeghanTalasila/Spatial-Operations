package edu.asu.cse512;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class ClosestPair {

	/**
	 * This method sorts the list of points by X Coordinates
	 * 
	 * @param listOfPoints
	 *            List<GeoPoint>
	 * @return Sorted list
	 */
	public static List<GeoPoint> sortByXcoordinate(List<GeoPoint> listOfPoints) {
		List<GeoPoint> copyList = new ArrayList<GeoPoint>(listOfPoints);
		Collections.sort(copyList, new Comparator<GeoPoint>() {
			public int compare(GeoPoint p1, GeoPoint p2) {
				if (p1.getX() < p2.getX()) {
					return -1;
				} else if (p1.getX() > p2.getX()) {
					return 1;
				} else {
					return 0;
				}
			}
		});
		return copyList;
	}

	/**
	 * This method sorts the list of points by Y Coordinates
	 * 
	 * @param listOfPoints
	 *            List<GeoPoint>
	 * @return SortedList
	 */
	public static List<GeoPoint> sortByYcoordinate(List<GeoPoint> listOfPoints) {
		List<GeoPoint> copyList = new ArrayList<GeoPoint>(listOfPoints);
		Collections.sort(copyList, new Comparator<GeoPoint>() {
			public int compare(GeoPoint p1, GeoPoint p2) {
				if (p1.getY() < p2.getY()) {
					return -1;
				} else if (p1.getY() > p2.getY()) {
					return 1;
				} else {
					return 0;
				}
			}
		});
		return copyList;
	}

	/**
	 * This method finds the closes pair using brute force to calculate
	 * distances between each pair
	 * 
	 * @param listOfPoints
	 * @return
	 */
	public static GeoPointPair recursionBaseCase(List<GeoPoint> listOfPoints) {
		int numOfPoints = listOfPoints.size();
		if (numOfPoints < 2) {
			return null;
		} else if (numOfPoints == 2) {
			GeoPointPair closestPair = new GeoPointPair(listOfPoints.get(0), listOfPoints.get(1));
			return closestPair;
		} else {
			GeoPointPair closestPair = new GeoPointPair(listOfPoints.get(0), listOfPoints.get(1));

			for (int i = 0; i < numOfPoints - 1; i++) {
				GeoPoint p1 = listOfPoints.get(i);
				for (int j = i + 1; j < numOfPoints; j++) {
					GeoPoint p2 = listOfPoints.get(j);
					double distance = GeoPoint.getDistance(p1, p2);
					if (distance < closestPair.getDistance()) {
						closestPair = new GeoPointPair(p1, p2);
					}
				}
			}
			return closestPair;
		}
	}

	public static GeoPointPair findClosestPair(List<GeoPoint> listOfPoints) {
		List<GeoPoint> sortedByX = sortByXcoordinate(listOfPoints);
		List<GeoPoint> sortedByY = sortByYcoordinate(listOfPoints);
		return closestPairRecursion(sortedByX, sortedByY);
	}

	private static GeoPointPair closestPairRecursion(List<GeoPoint> sortedByX, List<GeoPoint> sortedByY) {
		int numOfPoints = sortedByX.size();

		/*
		 * If the list is of small size, use brute force to compute distance and
		 * calculate the shortest pair
		 */
		if (numOfPoints <= 3) {
			return recursionBaseCase(sortedByX);
		} else {
			int pivot = numOfPoints >>> 1;
			List<GeoPoint> left = sortedByX.subList(0, pivot);
			List<GeoPoint> right = sortedByX.subList(pivot, numOfPoints);

			// Recurse on Left partition
			GeoPointPair closestPairInLeft = closestPairRecursion(left, sortByYcoordinate(left));

			// Recurse on Right partition
			GeoPointPair closestPairInRight = closestPairRecursion(right, sortByYcoordinate(right));

			/*
			 * Assign the closer of the closest pair in left and right
			 * partitions to the variable
			 */
			GeoPointPair closestPair;
			if (closestPairInLeft.getDistance() < closestPairInRight.getDistance()) {
				closestPair = closestPairInLeft;
			} else {
				closestPair = closestPairInRight;
			}

			double shortestDistance = closestPair.getDistance();
			GeoPoint center = right.get(0);
			List<GeoPoint> listOfClosePoints = new ArrayList<GeoPoint>();

			/*
			 * Find the points which might be closer to the closest pair using
			 * the difference in X
			 */
			for (GeoPoint point : sortedByY) {
				if (GeoPoint.getDistance(center, point) < shortestDistance) {
					listOfClosePoints.add(point);
				}
			}

			/*
			 * For each point which might be closer, find the difference in
			 * distance and re-assign the closestPair if necessary
			 */
			for (int i = 0; i < listOfClosePoints.size() - 1; i++) {
				GeoPoint p1 = listOfClosePoints.get(i);

				for (int j = i + 1; j < listOfClosePoints.size(); j++) {
					GeoPoint p2 = listOfClosePoints.get(j);

					if (GeoPoint.getDistance(p1, p2) >= shortestDistance) {
						break;
					} else {
						double distance = GeoPoint.getDistance(p1, p2);
						if (distance < closestPair.getDistance()) {
							closestPair = new GeoPointPair(p1, p2);
							shortestDistance = distance;
						}
					}
				}
			}
			return closestPair;
		}
	}

	/**
	 * This function finds the local pairs in the partitions
	 */
	private static final FlatMapFunction<Iterator<String>, GeoPointPair> LOCAL_CLOSEST_PAIR = new FlatMapFunction<Iterator<String>, GeoPointPair>() {

		private static final long serialVersionUID = -2199101645275752678L;

		public Iterable<GeoPointPair> call(Iterator<String> s) {

			List<GeoPoint> points = new ArrayList<GeoPoint>();
			while (s.hasNext()) {
				String strPoints = s.next();
				String[] rawPoints = strPoints.split(",");
				GeoPoint point = new GeoPoint(Double.parseDouble(rawPoints[0]), Double.parseDouble(rawPoints[1]));
				points.add(point);
			}
			// Find local closest pairs
			List<GeoPointPair> listOfPairs = new ArrayList<GeoPointPair>();
			GeoPointPair localClosestPair = findClosestPair(points);
			listOfPairs.add(localClosestPair);
			return listOfPairs;
		}
	};

	/**
	 * This function finds the final pair out of the local pair
	 */
	private static final FlatMapFunction<Iterator<GeoPointPair>, GeoPoint> FINAL_CLOSEST_PAIR = new FlatMapFunction<Iterator<GeoPointPair>, GeoPoint>() {

		private static final long serialVersionUID = -5793551749782404012L;

		public Iterable<GeoPoint> call(Iterator<GeoPointPair> givListIter) {
			List<GeoPoint> points = new ArrayList<GeoPoint>();
			while (givListIter.hasNext()) {
				GeoPointPair pair = givListIter.next();
				points.add(pair.getP());
				points.add(pair.getQ());
			}
			// Find global closest pairs from local ones
			GeoPointPair closestPair = findClosestPair(points);
			List<GeoPoint> finalPoints = new ArrayList<GeoPoint>();
			finalPoints.add(closestPair.getP());
			finalPoints.add(closestPair.getQ());
			Collections.sort(finalPoints, new Comparator<GeoPoint>() {
				@Override
				public int compare(GeoPoint p1, GeoPoint p2) {
					if (p1.getX() < p2.getX()) {
						return -1;
					} else if (p1.getX() > p2.getX()) {
						return 1;
					} else {
						return 0;
					}
				}
			});
			return finalPoints;
		}
	};

	/*
	 * Main function, take two parameter as input, output
	 * 
	 * @param inputLocation
	 * 
	 * @param outputLocation
	 * 
	 */
	public static void main(String[] args) {
		String inputFilename = args[0];
		String outputFilename = args[1];
		try {
			GeoSpatialUtils.deleteHDFSFile(outputFilename);

			SparkConf conf = new SparkConf().setAppName("Group20-ClosestPair");
			JavaSparkContext context = new JavaSparkContext(conf);
			JavaRDD<String> file = context.textFile(inputFilename);
			JavaRDD<GeoPointPair> localClosestPairs = file.mapPartitions(LOCAL_CLOSEST_PAIR).repartition(1);
			JavaRDD<GeoPoint> finalClosestPairPoints = localClosestPairs.mapPartitions(FINAL_CLOSEST_PAIR);
			finalClosestPairPoints.coalesce(1).saveAsTextFile(outputFilename);
			context.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
