package com.sunshine.kmeans;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class BaseCenterAwaredMapper extends Mapper<Text, Movie, Text, Movie> {

	protected List<Movie> centers = new ArrayList<Movie>();

	protected abstract String getCenterPathKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		centers.addAll(new CenterReader(conf.get(getCenterPathKey())).retrieveAllCenters());
	}

}