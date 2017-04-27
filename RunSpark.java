package at.ac.univie.spark;

import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


class RunSpark {

	class Rating extends Integer {};
	class TrackID extends Integer {};
	class AlbumID extends Integer {};

	// itemID, rating
	private static JavaPairRDD<TrackID, Rating> itemRatings;
	// trackid | albumid
	private static JavaPairRDD<TrackID, AlbumID> tracks;
	private static JavaPairRDD<Integer, Tuple2<Integer, Integer>> albumRatings; //<ItemID, <Rating, AlbumID>>

	public static void readData (String path, JavaSparkContext jsc) {

		// Format: <itemID>|<Rating>
		JavaRDD<String> data = jsc.textFile(path + "testIdx2.txt");

		// Parse to <int, int> (itemID, Rating)
		itemRatings = data
			.filter(s -> (!s.contains("|")))
			.mapToPair (
				s -> { 	String[] strarray = s.split("\t");
					return new Tuple2<TrackID, Rating>(TrackID.parseInt(strArray[0]), Rating.parseInt(strArray[1]));
				});
		


		// trackids

		// Format: <TrackId>|<albumID>|... (unimportant)
		JavaRDD<String> data2 = jsc.textFile(path + "trackData2_2.txt");
		tracks = data2.filter(s -> !s.contains("None"))
			     .mapToPair(s -> { 
				     strArray = s.split("\\|");
				     return new Tuple2<TrackID, AlbumID>(Integer.parseInt(strArray[0]), Integer.parseInt(strArray[0]));
			     }
			     );
	}


	public static void main(String [] args)
	{
		// on AWS:
		// SparkConf conf = new SparkConf().setAppName("GRUPPEXX");

		// local environment (laptop/PC)
		SparkConf conf = new SparkConf().setAppName("GRUPPE13").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		try {

			System.out.println(">>>>>>>>>>>>>>>>>> Hello from Spark! <<<<<<<<<<<<<<<<<<<<<<<<<");

			// path to folder with files
			String path = "/home/severin/Studium/SDM/aufgabe3/sparkSbt/src/main/resources/ydata-ymusic-kddcup-2011-track2/";

			readData(path, sc);

			// tracks: (trackid, albumID)
			// itemRatings: (trackID, Rating)

			albumRatings = itemRatings.join(tracks)
						  // (TrackID, (Rating, AlbumID)
						  .map(tuple -> tuple._2())
						  // (Rating, AlbumID)
					   	  .groupBy( tuple -> tuple._2())
						  // (AlbumID, [Rating, Rating, ...]
						  .map(tuple -> {
							  int size = tuple._2().size();
							  int rating = tuple._2().stream().reduce(0, Integer::sum);
							  return new Tuple2(tuple._1(), rating/size);
						  	})
						  // (AlbumID, AverageRating)


			albumRatings.foreach (
					tuple -> {
						System.out.println("
			new VoidFunction<Tuple2<Integer, Tuple2<Integer, Integer>>> () {
			    public void call (Tuple2<Integer, Tuple2<Integer, Integer>> t) {
				System.out.println (t._1() + " " + t._2()._1() + " " +  t._2()._2());
			    }
			}
		    );
			
			System.out.println(itemRatings.count());
			System.out.println(tracks.count());

		} catch (Exception e) {
			System.out.println(e);
		}

		sc.close();
	}

}

