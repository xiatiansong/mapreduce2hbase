package com.sunshine.kmeans;

import static com.sunshine.kmeans.KmeansUtils.*;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class KmeansCenterMapper extends BaseCenterAwaredMapper {

	private Text centerKey = new Text();

	@Override
	protected void map(Text key, Movie movie, Context context) throws IOException, InterruptedException {
		Movie closestCenter = null;
		double closestDistance = Double.MAX_VALUE;
		for (Movie center : centers) {
			double _distance = kmeansDistance(movie, center);
			if (_distance < closestDistance) {
				closestCenter = center;
				closestDistance = _distance;
			}
		}
		// System.out.println("Clustering movie: " + movie.getMovieId() +
		// " to center: " + closestCenter.getMovieId());
		centerKey.set(closestCenter.getMovieId());
		context.write(centerKey, movie);
	}

	@Override
	protected String getCenterPathKey() {
		return KMEANS_CENTER_PATH_KEY;
	}

}
