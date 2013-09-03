package com.sunshine.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansClusterReducer extends Reducer<Text, Movie, Text, Text> {

	private Text clusterMovies = new Text();

	@Override
	protected void reduce(Text clusterId, Iterable<Movie> movies, Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		for (Movie movie : movies) {
			builder.append(movie.getMovieId()).append(", ");
		}
		builder.delete(builder.length() - 2, builder.length());
		clusterMovies.set(builder.toString());
		context.write(clusterId, clusterMovies);
	}
}
