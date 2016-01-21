package edu.asu.cse512;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

public class Union {

	private static String sparkMasterIP;

	private static final FlatMapFunction<Iterator<String>, Geometry> UNION = new FlatMapFunction<Iterator<String>, Geometry>() {

		private static final long serialVersionUID = -1700297168222641028L;

		@Override
		public Iterable<Geometry> call(Iterator<String> strIterator) {

			List<Geometry> listOfPolygons = new ArrayList<Geometry>();
			GeometryFactory geom = new GeometryFactory();

			/*
			 * Read through each of the file lines and create coordinates of the
			 * polygon
			 */
			while (strIterator.hasNext()) {
				String rawCoordinates = strIterator.next();
				String[] individualCoordinates = rawCoordinates.split(",");
				Double x1 = Double.parseDouble(individualCoordinates[0]);
				Double y1 = Double.parseDouble(individualCoordinates[1]);
				Double x2 = Double.parseDouble(individualCoordinates[2]);
				Double y2 = Double.parseDouble(individualCoordinates[3]);

				Coordinate c1 = new Coordinate(x1, y1);
				Coordinate c2 = new Coordinate(x1, y2);
				Coordinate c3 = new Coordinate(x2, y2);
				Coordinate c4 = new Coordinate(x2, y1);

				// Create polygon coordinates
				Coordinate[] coordinates = new Coordinate[] { c1, c2, c3, c4, c1 };

				// Create and add the polygon
				Geometry polygon = geom.createPolygon(coordinates);
				listOfPolygons.add(polygon);
			}

			/*
			 * Run the Cascaded Polygon Union algorithm and add the resultant
			 * polygons to the list
			 */
			CascadedPolygonUnion cascadedPolygons = new CascadedPolygonUnion((Collection<Geometry>) listOfPolygons);
			Geometry finalPolygons = cascadedPolygons.union();

			List<Geometry> listOfFinalPolygons = new ArrayList<Geometry>();

			if (finalPolygons != null) {
				for (int i = 0; i < finalPolygons.getNumGeometries(); i++) {
					listOfFinalPolygons.add((Geometry) finalPolygons.getGeometryN(i));
				}
			}

			return listOfFinalPolygons;
		}
	};

	private static final FlatMapFunction<Iterator<Geometry>, String> FINAL_UNION = new FlatMapFunction<Iterator<Geometry>, String>() {

		private static final long serialVersionUID = -786100233543841224L;

		public Iterable<String> call(Iterator<Geometry> geoIterator) {

			List<Geometry> listOfPolygons = new ArrayList<Geometry>();

			// Iterate through the input polygons
			while (geoIterator.hasNext()) {
				listOfPolygons.add(geoIterator.next());
			}

			/*
			 * Run the Cascaded Polygon Union algorithm and add the results to
			 * the list
			 */
			CascadedPolygonUnion cascadedPolygons = new CascadedPolygonUnion((Collection<Geometry>) listOfPolygons);
			Geometry finalPolygons = cascadedPolygons.union();

			List<Coordinate> listOfFinalPolygonCoords = new ArrayList<Coordinate>();

			if (finalPolygons != null) {
				for (int i = 0; i < finalPolygons.getNumGeometries(); i++) {
					Coordinate[] coords = ((Geometry) finalPolygons.getGeometryN(i)).getCoordinates();
					for (Coordinate c : coords) {
						listOfFinalPolygonCoords.add(c);
					}
				}
			}

			// Create a list of distinct points and sort it
			List<Coordinate> distinctPoints = new ArrayList<Coordinate>(
					new HashSet<Coordinate>(listOfFinalPolygonCoords));
			Collections.sort(distinctPoints);

			// Create a string representation of the coordinates
			List<String> stringOutput = new ArrayList<String>();
			for (Coordinate c : distinctPoints) {
				stringOutput.add(String.format("%s,%s", c.x, c.y));
			}

			return stringOutput;
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

			SparkConf conf = new SparkConf().setAppName("Group20-Union");
			JavaSparkContext context = new JavaSparkContext(conf);
			JavaRDD<String> file = context.textFile(inputFilename);
			JavaRDD<Geometry> geometricalMap = file.mapPartitions(UNION);
			JavaRDD<Geometry> geometricalUnions = geometricalMap.repartition(1);
			JavaRDD<String> geometricalUnionResult = geometricalUnions.mapPartitions(FINAL_UNION);
			geometricalUnionResult.coalesce(1).saveAsTextFile(outputFilename);
			context.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
