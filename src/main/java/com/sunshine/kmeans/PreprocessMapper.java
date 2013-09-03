package com.sunshine.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PreprocessMapper extends Mapper<Object, Text, Text, Text> {

	private Text movieId = new Text();
	private Text outputValue = new Text();

	@Override
	protected void map(Object key, Text value, Context output) throws IOException, InterruptedException {
		try {
			doMap(key, value, output);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void doMap(Object key, Text value, Context output) throws IOException, InterruptedException {
		movieId.set(((FileSplit) output.getInputSplit()).getPath().getName());
		String[] infos = value.toString().split(",");
		if (infos.length != 3) {
			return;
		}
		outputValue.set(infos[0] + ":" + infos[1]);
		output.write(movieId, outputValue);
	}

}
