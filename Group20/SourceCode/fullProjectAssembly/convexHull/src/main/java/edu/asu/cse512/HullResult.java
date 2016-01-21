package edu.asu.cse512;

import org.apache.spark.api.java.JavaRDD;


public class HullResult {
	private final JavaRDD<GeoPoint> upperHull;
	private final JavaRDD<GeoPoint> lowerHull;
	
	public HullResult(JavaRDD<GeoPoint> initUpperHull, JavaRDD<GeoPoint> initLowerHull)
	{
		this.upperHull = initUpperHull;
		this.lowerHull = initLowerHull;
	}

	public JavaRDD<GeoPoint> lowerHull()
	{
		return this.lowerHull;
	}

	public JavaRDD<GeoPoint> upperHull()
	{
		return this.upperHull;
	}
}