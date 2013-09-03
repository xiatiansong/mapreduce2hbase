package com.sunshine.kmeans;

import static com.sunshine.kmeans.KmeansUtils.INTERATION_NUM;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansCenterReducer extends Reducer<Text, Movie, Text, Movie> {

	private Text centerKey = new Text();

	@Override
	protected void reduce(Text key, Iterable<Movie> movies, Context context) throws IOException, InterruptedException {
		Movie center = KmeansUtils.average(movies);
		center.setMovieId(key.toString() + "_" + context.getConfiguration().getInt(INTERATION_NUM, 0));
		// System.out.println("Refreshing center: " + center);
		centerKey.set(center.getMovieId());
		context.write(centerKey, center);
	}
}
