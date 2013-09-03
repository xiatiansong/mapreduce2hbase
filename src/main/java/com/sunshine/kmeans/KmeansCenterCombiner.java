package com.sunshine.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansCenterCombiner extends Reducer<Text, Movie, Text, Movie> {

	@Override
	protected void reduce(Text key, Iterable<Movie> movies, Context context) throws IOException, InterruptedException {
		Movie center = KmeansUtils.average(movies);
		center.setMovieId(key.toString());
		// System.out.println("Refreshing center: " + center);
		context.write(key, center);
	}
}
