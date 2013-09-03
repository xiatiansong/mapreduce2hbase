package com.sunshine.kmeans;

import static com.sunshine.kmeans.KmeansUtils.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CanopyCenterReducer extends Reducer<Text, Movie, Text, Movie> {

	private Text centerKey = new Text();
	private List<Movie> centers = new ArrayList<Movie>();

	@Override
	protected void reduce(Text key, Iterable<Movie> movies, Context context) throws IOException, InterruptedException {
		for (Movie movie : movies) {
			// System.out.println("Considering adding movie to centers: " +
			// movie.getMovieId());
			boolean stronglyAssociated = false;
			for (Movie center : centers) {
				if (isStronglyAssociatedToCenter(movie, center)) {
					stronglyAssociated = true;
				}
			}
			if (!stronglyAssociated) {
				centers.add(movie.getCloneMovie());
			}
			else {
				// System.out.println("Rejecting movie from adding to centers: "
				// + movie.getMovieId());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (int i = 0; i < centers.size(); i++) {
			Movie movie = centers.get(i);
			for (int j = i; j < centers.size(); j++) {
				Movie center = centers.get(j);
				if (isWeaklyAssociatedToCenter(movie, center)) {
					movie.addCanopyCenter(center.getMovieId());
				}
			}
			// System.out.println("Adding movie to centers: " + movie);
			centerKey.set(movie.getMovieId());
			context.write(centerKey, movie);
		}
	}

}
