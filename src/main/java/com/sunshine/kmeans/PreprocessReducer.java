package com.sunshine.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessReducer extends Reducer<Text, Text, Text, Movie> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			doReduce(key, values, context);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Movie movie = new Movie();
		movie.setMovieId(key.toString());
		for (Text value : values) {
			String[] infos = value.toString().split(":");
			movie.addRating(infos[0], Double.parseDouble(infos[1]));
		}
		movie.setWeight(1);
		context.write(key, movie);
		// System.out.println("Writing movie: " + movie);
	}

}
