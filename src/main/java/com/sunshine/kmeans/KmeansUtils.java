package com.sunshine.kmeans;

import java.util.*;

public class KmeansUtils {

	// TS

	public static final long TS = System.currentTimeMillis();

	// PATH

	public static final String PRE_PROCESS_INPUT_PATH = "/home/hadoop/Kmeans/input";

	public static final String PRE_PROCESS_OUTPUT_PATH = "/home/hadoop/Kmeans/output" + TS + "/preProcess";

	public static final String CANOPY_CENTER_OUTPUT_PATH = "/home/hadoop/Kmeans/output" + TS + "/canopyCenter";

	public static final String CANOPY_CLUSTER_OUTPUT_PATH = "/home/hadoop/Kmeans/output" + TS + "/canopyCluster";

	public static final String KMEANS_CENTER_OUTPUT_PATH = "/home/hadoop/Kmeans/output" + TS + "/kmeansCenter";

	public static final String KMEANS_CLUSTER_OUTPUT_PATH = "/home/hadoop/Kmeans/output" + TS + "/kmeansCluster";

	// KEY

	public static final String CENTER_KEY = "center";

	public static final String INPUT_PATH_KEY = "input-path";

	public static final String OUTPUT_PATH_KEY = "output-path";

	public static final String INTERATION_NUM = "interation-num";

	public static final String CANOPY_CENTER_PATH_KEY = "canpoy-center-path-key";

	public static final String KMEANS_CENTER_PATH_KEY = "kmeans-center-path-key";

	public static int canopyDistance(Movie movier, Movie moviee) {
		int distance = 0;
		for (String user : movier.getAllUsers()) {
			if (moviee.hasUserRated(user)) {
				distance++;
			}
		}
		return distance;
	}

	public static boolean isStronglyAssociatedToCenter(Movie movie, Movie center) {
		return canopyDistance(movie, center) >= 8;
	}

	public static boolean isWeaklyAssociatedToCenter(Movie movie, Movie center) {
		return canopyDistance(movie, center) >= 2;
	}

	public static double kmeansDistance(Movie movier, Movie moviee) {
		double distance = 0;
		if (!movier.hasCommonCanopy(moviee)) {
			return Double.MAX_VALUE;
		}
		Map<String, Double> matrix = new HashMap<String, Double>();
		for (String userId : movier.getAllUsers()) {
			matrix.put(userId, movier.getUserRating(userId));
		}
		for (String userId : moviee.getAllUsers()) {
			matrix.put(userId, moviee.getUserRating(userId) - (matrix.containsKey(userId) ? matrix.get(userId) : 0));
		}
		for (Double value : matrix.values()) {
			distance += value * value;
		}
		return distance;
	}

	public static Movie average(Iterable<Movie> movies) {
		Set<String> canopyCenters = new HashSet<String>();
		Map<String, Double> matrix = new HashMap<String, Double>();
		int size = 0;
		for (Movie movie : movies) {
			for (String userId : movie.getAllUsers()) {
				matrix.put(userId, movie.getUserRating(userId) * movie.getWeight() + (matrix.containsKey(userId) ? matrix.get(userId) : 0));
			}
			if (canopyCenters.size() == 0) {
				canopyCenters.addAll(Arrays.asList(movie.getAllCanopyCenters()));
			}
			canopyCenters.retainAll(Arrays.asList(movie.getAllCanopyCenters()));
			size += movie.getWeight();
		}
		Movie average = new Movie();
		for (String canopyCenterId : canopyCenters) {
			average.addCanopyCenter(canopyCenterId);
		}
		for (Map.Entry<String, Double> rinfo : matrix.entrySet()) {
			average.addRating(rinfo.getKey(), rinfo.getValue() / size);
		}
		average.setWeight(size);
		return average;
	}

	public static boolean hasConverge(List<Movie> preCenters, List<Movie> postCenters) {
		for (Movie preMovie : preCenters) {
			double distance = Double.MAX_VALUE;
			for (Movie postMovie : postCenters) {
				double _d = kmeansDistance(preMovie, postMovie);
				distance = _d < distance ? _d : distance;
			}
			if (distance > 5)
				return false;
		}
		return true;
	}
}
