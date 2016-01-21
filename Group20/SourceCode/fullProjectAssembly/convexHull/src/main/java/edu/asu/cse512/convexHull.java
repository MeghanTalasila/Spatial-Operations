package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class convexHull implements Serializable 
{
	private static final long serialVersionUID = -205075934063918279L;

	class SortPoints implements Function<GeoPoint,GeoPoint>, Serializable
	{
		private static final long serialVersionUID = 8225302338329281442L;

		public GeoPoint call(GeoPoint geoPoint) throws Exception {
			return geoPoint;
		}
	}

	class ParsePoints implements Function<String, GeoPoint>, Serializable
	{
		private static final long serialVersionUID = -3885195256936448019L;

		public GeoPoint call(String inputLine)
		{
			return new GeoPoint(inputLine);
		}
	}

	class CalculateHull implements FlatMapFunction<Iterator<GeoPoint>, GeoPoint>, Serializable
	{
		private static final long serialVersionUID = -4620305040646468710L;

		public Iterable<GeoPoint> call(Iterator<GeoPoint> geoPoints) throws Exception {
			List<GeoPoint> newGeoPoints = new ArrayList<GeoPoint>();

			// Handle the zero points case.
			if (!geoPoints.hasNext()) 
				return newGeoPoints; 

			// First point is always on hull.
			GeoPoint firstPoint = geoPoints.next();
			newGeoPoints.add(firstPoint);

			if (!geoPoints.hasNext())
				return newGeoPoints; 
			GeoPoint secondPoint = geoPoints.next();

			while (geoPoints.hasNext()) {
				GeoPoint thirdPoint = geoPoints.next();

				// If the slope from first to third is less than the slope from first
				//  to second, then use second.  Second becomes basis for next points.
				//  If not, then don't save second, and keep first as basis for next
				//  comparison.
				double firstThirdSlope = firstPoint.getSlope(thirdPoint);
				double firstSecondSlope = firstPoint.getSlope(secondPoint);

				boolean	useSecond = false;

				// Handle the special case of vertical points, where slope would be infinite.
				if (firstPoint.getX() == secondPoint.getX())
				{
					// Only use a vertical pairs of points if there are 3 vertical points in a row.
					if (firstPoint.getX() == thirdPoint.getX())
						useSecond = true;
				}
				// Only check slopes if firstSecond isn't infinite
				else if (firstThirdSlope <= firstSecondSlope)
					useSecond = true;

				// So now we use the second point, and make the second point the basis for comparison.
				if (useSecond)
				{
					newGeoPoints.add(secondPoint);
					firstPoint = secondPoint;
				}
				secondPoint = thirdPoint;
			}

			// Last point is always on hull. 
			newGeoPoints.add(secondPoint);

			return newGeoPoints;
		}
	}

	public HullResult calculateConvexHull(JavaRDD<GeoPoint> geoPoints)
	{
		// Upper hull.

		JavaRDD<GeoPoint> upperGeoPoints = geoPoints.sortBy(new SortPoints(), true, 1);
		JavaRDD<GeoPoint> newGeoPoints;

		do {
			newGeoPoints = upperGeoPoints;
			upperGeoPoints = newGeoPoints.mapPartitions(new CalculateHull());
		} while (newGeoPoints.count() != upperGeoPoints.count());

		// lower hull.

		JavaRDD<GeoPoint> lowerGeoPoints = geoPoints.sortBy(new SortPoints(), false, 1);
		do {
			newGeoPoints = lowerGeoPoints;
			lowerGeoPoints = newGeoPoints.mapPartitions(new CalculateHull());
		} while (newGeoPoints.count() != lowerGeoPoints.count());

		return new HullResult(upperGeoPoints, lowerGeoPoints);
	}


	public void operation(String inputFilePath, String outputFilePath)
	{
		SparkConf sc = new SparkConf().setAppName("Group20-ConvexHull");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		JavaRDD<String> inputStrings = jsc.textFile(inputFilePath);

		JavaRDD<GeoPoint> geoPoints = inputStrings.map(new ParsePoints()); 
		HullResult convexHullResult = calculateConvexHull(geoPoints);

		// Return the union of the upper and lower hulls, with the repeated elements removed.
		JavaRDD<GeoPoint> resultGeoPoints = convexHullResult.upperHull().union(convexHullResult.lowerHull()).distinct(1).sortBy(new SortPoints(), true, 1);

		GeoSpatialUtils.deleteHDFSFile(outputFilePath);
		resultGeoPoints.saveAsTextFile(outputFilePath);  

		jsc.close();
	}

	public static void main(String[] args) {
		new convexHull().operation(args[0], args[1]);
	}
}