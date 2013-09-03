package com.sunshine.kmeans;

import static com.sunshine.kmeans.KmeansUtils.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class KmeansDriver {

	public static void main(String[] args) throws Exception {
		boolean result = false;

		Configuration preProcessConf = new Configuration();
		preProcessConf.set(INPUT_PATH_KEY, PRE_PROCESS_INPUT_PATH);
		preProcessConf.set(OUTPUT_PATH_KEY, PRE_PROCESS_OUTPUT_PATH);

		result = preprocess(preProcessConf);
		if (!result) {
			System.exit(1);
		}

		Configuration canopyCenterConf = new Configuration();
		canopyCenterConf.set(INPUT_PATH_KEY, PRE_PROCESS_OUTPUT_PATH);
		canopyCenterConf.set(OUTPUT_PATH_KEY, CANOPY_CENTER_OUTPUT_PATH);

		result = canopyCenter(canopyCenterConf);
		if (!result) {
			System.exit(1);
		}

		Configuration canopyClusterConf = new Configuration();
		canopyClusterConf.set(INPUT_PATH_KEY, PRE_PROCESS_OUTPUT_PATH);
		canopyClusterConf.set(OUTPUT_PATH_KEY, CANOPY_CLUSTER_OUTPUT_PATH);
		canopyClusterConf.set(CANOPY_CENTER_PATH_KEY, CANOPY_CENTER_OUTPUT_PATH);

		result = canopyCluster(canopyClusterConf);
		if (!result) {
			System.exit(1);
		}

		int _t = 0;
		Configuration kmeansCenterConf = new Configuration();
		kmeansCenterConf.set(INPUT_PATH_KEY, CANOPY_CLUSTER_OUTPUT_PATH);
		kmeansCenterConf.set(OUTPUT_PATH_KEY, KMEANS_CENTER_OUTPUT_PATH + "_" + _t);
		kmeansCenterConf.set(KMEANS_CENTER_PATH_KEY, CANOPY_CENTER_OUTPUT_PATH);
		kmeansCenterConf.setInt(INTERATION_NUM, _t);

		result = kmeansCenter(kmeansCenterConf);
		if (!result) {
			System.exit(1);
		}

		while (_t < 4 && needMoreInteration(kmeansCenterConf)) {
			_t++;

			kmeansCenterConf = new Configuration();
			kmeansCenterConf.set(INPUT_PATH_KEY, CANOPY_CLUSTER_OUTPUT_PATH);
			kmeansCenterConf.set(OUTPUT_PATH_KEY, KMEANS_CENTER_OUTPUT_PATH + "_" + _t);
			kmeansCenterConf.set(KMEANS_CENTER_PATH_KEY, KMEANS_CENTER_OUTPUT_PATH + "_" + (_t - 1));
			kmeansCenterConf.setInt(INTERATION_NUM, _t);

			result = kmeansCenter(kmeansCenterConf);
			if (!result) {
				System.exit(1);
			}
		}

		Configuration kmeansClusterConf = new Configuration();
		kmeansClusterConf.set(INPUT_PATH_KEY, CANOPY_CLUSTER_OUTPUT_PATH);
		kmeansClusterConf.set(KMEANS_CENTER_PATH_KEY, KMEANS_CENTER_OUTPUT_PATH + "_" + _t);
		kmeansClusterConf.set(OUTPUT_PATH_KEY, KMEANS_CLUSTER_OUTPUT_PATH);

		result = kmeansCluster(kmeansClusterConf);
		System.exit(result ? 0 : 1);
	}

	public static boolean needMoreInteration(Configuration conf) {
		List<Movie> preCenters = new CenterReader(conf.get(KMEANS_CENTER_PATH_KEY)).retrieveAllCenters();
		List<Movie> postCenters = new CenterReader(conf.get(OUTPUT_PATH_KEY)).retrieveAllCenters();
		return !hasConverge(preCenters, postCenters);
	}

	public static boolean preprocess(Configuration conf) throws Exception {
		Job job = new Job(conf, "Preprocess");
		job.setJobName("Preprocess");
		job.setMapperClass(PreprocessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(PreprocessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(KmeansDriver.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get(INPUT_PATH_KEY)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_PATH_KEY)));

		return job.waitForCompletion(true);
	}

	public static boolean canopyCenter(Configuration conf) throws Exception {
		Job job = new Job(conf, "Canopy Center");
		job.setJobName("Canopy Center");
		job.setMapperClass(CanopyCenterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(CanopyCenterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(KmeansDriver.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get(INPUT_PATH_KEY)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_PATH_KEY)));

		return job.waitForCompletion(true);
	}

	public static boolean canopyCluster(Configuration conf) throws Exception {
		Job job = new Job(conf, "Canopy Cluster");
		job.setJobName("Canopy Cluster");
		job.setMapperClass(CanopyClusterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(KmeansDriver.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get(INPUT_PATH_KEY)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_PATH_KEY)));

		return job.waitForCompletion(true);
	}

	private static boolean kmeansCenter(Configuration conf) throws Exception {
		int interation_num = conf.getInt(INTERATION_NUM, 0);
		Job job = new Job(conf, "Kmeans Center _" + interation_num);
		job.setJobName("Kmeans Center _" + interation_num);
		job.setMapperClass(KmeansCenterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setCombinerClass(KmeansCenterCombiner.class);
		job.setReducerClass(KmeansCenterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(KmeansDriver.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get(INPUT_PATH_KEY)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_PATH_KEY)));

		return job.waitForCompletion(true);
	}

	private static boolean kmeansCluster(Configuration conf) throws Exception {
		Job job = new Job(conf, "Kmeans Cluster");
		job.setJobName("Kmeans Cluster");
		job.setMapperClass(KmeansClusterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(KmeansClusterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(KmeansDriver.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get(INPUT_PATH_KEY)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_PATH_KEY)));

		return job.waitForCompletion(true);
	}

}
