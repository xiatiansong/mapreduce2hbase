package com.sunshine.kmeans;

import static com.sunshine.kmeans.KmeansUtils.*;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class CanopyClusterMapper extends BaseCenterAwaredMapper {

	@Override
	protected void map(Text key, Movie movie, Context context) throws IOException, InterruptedException {
		for (Movie center : centers) {
			if (isWeaklyAssociatedToCenter(movie, center)) {
				movie.addCanopyCenter(center.getMovieId());
			}
		}
		// System.out.println("Updating movie: " + movie);
		context.write(key, movie);
	}

	@Override
	protected String getCenterPathKey() {
		return CANOPY_CENTER_PATH_KEY;
	}

}
