package com.sunshine.kmeans;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class Movie implements Writable {

	private String movieId;
	private List<String> canopyCenters = new ArrayList<String>();
	private Map<String, Double> ratings = new LinkedHashMap<String, Double>();
	private int weight;

	public Movie() {
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public void addCanopyCenter(String center) {
		canopyCenters.add(center);
	}

	public String[] getAllCanopyCenters() {
		return canopyCenters.toArray(new String[] {});
	}

	public boolean hasCommonCanopy(Movie moviee) {
		for (String _center : moviee.getAllCanopyCenters()) {
			if (canopyCenters.contains(_center)) {
				return true;
			}
		}
		return false;
	}

	public void addRating(String userId, Double rating) {
		ratings.put(userId, rating);
	}

	public boolean hasUserRated(String userId) {
		return ratings.containsKey(userId);
	}

	public String[] getAllUsers() {
		return ratings.keySet().toArray(new String[] {});
	}

	public double getUserRating(String userId) {
		return ratings.get(userId);
	}

	public void clear() {
		movieId = null;
		canopyCenters.clear();
		ratings.clear();
		weight = 1;
	}

	public void fromFormatString(String input) {
		clear();
		int findex = input.indexOf('|');
		setMovieId(input.substring(1, findex));
		int sindex = input.indexOf('|', findex + 1);
		String[] rinfos = input.substring(findex + 1, sindex).split("<|>");
		for (String rinfo : rinfos) {
			if (rinfo != null && rinfo.length() > 0) {
				String[] ratingInfos = rinfo.split(":");
				addRating(ratingInfos[0], Double.parseDouble(ratingInfos[1]));
			}
		}
		int tindex = input.indexOf('|', sindex + 1);
		String[] cinfos = input.substring(sindex + 1, tindex).split("<|>");
		for (String cinfo : cinfos) {
			if (cinfo != null && cinfo.length() > 0) {
				addCanopyCenter(cinfo);
			}
		}
		int winfo = Integer.parseInt(input.substring(tindex + 1, input.length() - 1));
		setWeight(winfo);
	}

	public String asFormatString() {
		StringBuilder stringToBuild = new StringBuilder();
		stringToBuild.append('[');
		stringToBuild.append(movieId).append('|');
		for (Map.Entry<String, Double> entry : ratings.entrySet()) {
			stringToBuild.append('<').append(entry.getKey()).append(':').append(entry.getValue()).append('>');
		}
		stringToBuild.append('|');
		for (String center : canopyCenters) {
			stringToBuild.append('<').append(center).append('>');
		}
		stringToBuild.append('|');
		stringToBuild.append(weight);
		stringToBuild.append(']');
		return stringToBuild.toString();
	}

	public void readFields(DataInput input) throws IOException {
		int length = WritableUtils.readVInt(input);
		byte[] bytes = new byte[length];
		input.readFully(bytes, 0, length);
		fromFormatString(new String(bytes));
	}

	public void write(DataOutput output) throws IOException {
//		byte[] bytes = asFormatString().getBytes();
//		int length = bytes.length;
//		WritableUtils.writeVInt(output, length);
//		output.write(bytes, 0, length);
		String content = asFormatString();
		WritableUtils.writeVInt(output, content.length());
		output.writeBytes(content);
	}

	public void fromString(String input) {
		fromFormatString(input);
	}

	@Override
	public String toString() {
		return asFormatString();
	}

	public Movie getCloneMovie() {
		String content = asFormatString();
		Movie clone = new Movie();
		clone.fromFormatString(content);
		return clone;
	}

}
