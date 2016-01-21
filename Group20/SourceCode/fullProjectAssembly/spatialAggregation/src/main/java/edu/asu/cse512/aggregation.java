package edu.asu.cse512;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class aggregation implements Serializable {
	private static final long serialVersionUID = -78390015906582065L;

	public void spatialAggregationMain(String inpFile, String outFile) {

		SparkConf conf = new SparkConf().setAppName("Aggregation");

		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> lines=context.textFile(inpFile + "/part*").flatMap(new FlatMapFunction<String, String>() {
     			private static final long serialVersionUID = 5808683219875436059L;

			public Iterable<String> call(String s) {
                return Arrays.asList(s);
            }
        });

		JavaPairRDD<String,Integer> result = lines.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 23401465774896120L;

					public Tuple2<String, Integer> call(String s) {
						String[] inputLine = s.split(",");
						return new Tuple2<String, Integer>(inputLine[0] , (inputLine.length - 1));
					}
				});

		JavaPairRDD<String, Integer> counts=result.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = -8509104802484541121L;

					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});
		JavaRDD<String> finalResult =counts.sortByKey(true).map(new Function<Tuple2<String,Integer>,String>(){
			private static final long serialVersionUID = -3518147642478352203L;

			@Override
			public String call(Tuple2<String, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._1()+","+v1._2();
			}
			
			
		});
		finalResult.coalesce(1).saveAsTextFile(outFile);
		context.close();
	}
	

	public static void main(String args[]) {
		String joinPutFile = args[2] + "temp";
		// Initialize, need to remove existing in output file location.
		Join join = new Join();
		join.spatialJoinMain(args[0], args[1], joinPutFile, args[3]);
		new aggregation().spatialAggregationMain(joinPutFile, args[2]);
	}
}
