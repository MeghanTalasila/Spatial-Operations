package edu.asu.cse512;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class RangeQuery implements Serializable {

	private static final long serialVersionUID = -7347795517571950L;

	public void rangeQuery(String input1, String input2, String output) {
		SparkConf sc = new SparkConf().setAppName("Group20-RangeQuery");
		JavaSparkContext context = new JavaSparkContext(sc);
		JavaRDD<String> file1 = context.textFile(input1);
		JavaRDD<String> file2 = context.textFile(input2);

		JavaRDD<GeoPoint> points = file1.map(new Function<String, GeoPoint>() {
			private static final long serialVersionUID = 4103513079613043110L;
			public GeoPoint call(String str) throws Exception {
				String[] input_temp = str.split(",");
				if (input_temp.length > 2) {
					int id = Integer.parseInt(input_temp[0]);
					double x1 = Double.parseDouble(input_temp[1]);
					double y1 = Double.parseDouble(input_temp[2]);
					GeoPoint geopoint = new GeoPoint(id, x1, y1);
					return geopoint;
				}
				return null;
			}
		});		

		JavaRDD<Rectangle> queryWindow = file2.map(new Function<String, Rectangle>() {
			private static final long serialVersionUID = -3281992130910139220L;
			public Rectangle call(String s) throws Exception {
				String[] input_temp = s.split(",");
				double x1, y1, x2, y2;
				if (input_temp.length > 3) {
					x1 = Double.parseDouble(input_temp[0]);
					y1 = Double.parseDouble(input_temp[1]);
					x2 = Double.parseDouble(input_temp[2]);
					y2 = Double.parseDouble(input_temp[3]);
					Rectangle rectangle = new Rectangle(x1, y1, x2, y2);
					return rectangle;
				}
				return null;
			}
		});	

		final Broadcast<Rectangle> cachedWindow = context.broadcast(queryWindow.first());
		JavaRDD<Integer> result=points.map(new Function<GeoPoint,Integer>(){
			private static final long serialVersionUID = -6730090128779842348L;
			@Override
			public Integer call(GeoPoint p) throws Exception {
				Integer id=null;
				if(cachedWindow.value().containsPoints(p))
				{
					id=p.getId();
				}
				return id;
			}			
		});

		JavaRDD<Integer> filteredRdd =result.filter(RemoveSpaces);
		JavaRDD<Integer> sortedFilteredRDD=filteredRdd.sortBy(new SortInteger(), true, 1);
		sortedFilteredRDD.coalesce(1).saveAsTextFile(output);
		context.close();
	}

	class SortInteger implements Function<Integer,Integer>, Serializable
	{

		private static final long serialVersionUID = 4233925782208790537L;

		public Integer call(Integer str) throws Exception {
			return str;
		}
	}


	public final static Function<Integer, Boolean> RemoveSpaces = new Function<Integer, Boolean>() {

		private static final long serialVersionUID = 2996151472825906536L;

		public Boolean call(Integer s) {
			if(s!=null){
				return true;
			}
			return false;

		}
	};

	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	 */
	public static void main( String[] args )
	{
		//Initialize, need to remove existing in output file location.
		GeoSpatialUtils.deleteHDFSFile(args[2]);

		//Implement 

		RangeQuery rangeQuery = new RangeQuery();
		rangeQuery.rangeQuery(args[0], args[1], args[2]);

		//Output your result, you need to sort your result!!!
		//And,Don't add a additional clean up step delete the new generated file...

	}
}
