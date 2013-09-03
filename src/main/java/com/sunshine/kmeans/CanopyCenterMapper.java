package com.sunshine.kmeans;

import static com.sunshine.kmeans.KmeansUtils.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CanopyCenterMapper extends Mapper<Text, Movie, Text, Movie> {

	private Text centerKey = new Text(CENTER_KEY);
	private List<Movie> centers = new ArrayList<Movie>();

	@Override
	protected void map(Text key, Movie movie, Context context) throws IOException, InterruptedException {
		
		boolean stronglyAssociated = false;
		for (Movie center : centers) {
			if (isStronglyAssociatedToCenter(movie, center)) {
				stronglyAssociated = true;
			}
		}
		if (!stronglyAssociated) {
			centers.add(movie.getCloneMovie());
		
			context.write(centerKey, movie);
		}
		else {
		}
	}

}
