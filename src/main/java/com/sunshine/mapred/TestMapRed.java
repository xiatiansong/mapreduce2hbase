package com.sunshine.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestMapRed {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(TestMapRed.class);
	    job.setMapperClass(MapperClass.class);
	    job.setCombinerClass(ReducerClass.class);
	    job.setReducerClass(ReducerClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/data/ch04/mapred_02.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("/output/201305111423"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
