package com.sunshine.hadoopaction;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestChongFu {
		
	/**
	 * Enumeration for Hadoop error counters.
	 */
	private enum COUNTER_KEYS {
		INVALID_LINES,CHONGFU
	};
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().trim().isEmpty()) { // ignore empty lines
				context.getCounter(COUNTER_KEYS.INVALID_LINES).increment(1);
				return;
			}
			String[] splitValues = value.toString().split(" ");
			String date = splitValues[0];
			String[] dates = date.split("-");
			if(dates[1].length() == 1){
				date = dates[0] +"-0"+ dates[1];
			}else{
				date = dates[0] +"-"+ dates[1];
			}
			if(dates[2].length() == 1){
				date += "-0"+ dates[2];
			}else{
				date += "-"+ dates[2];
			}
			context.write(new Text(date), new Text(splitValues[1]));
		}
	}
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Iterator<Text> iterator = values.iterator();
			Set<Text> set = new HashSet<Text>();
			StringBuffer value = new StringBuffer();
			while(iterator.hasNext()){
				value.append(iterator.next().toString()+",");
				//Text val = iterator.next();
				
				/*if(!set.contains(val)){
					set.add(val);
					context.write(key, val);
				}else{
					context.getCounter(COUNTER_KEYS.CHONGFU).increment(1);
				}*/
			}
			context.write(key, new Text(value.toString()));
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		Job job = new Job(conf, " quchongfu Job");
		job.setJarByClass(TestChongFu.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/input/file1.txt"));
		FileInputFormat.addInputPath(job, new Path("/input/file2.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/output/201306141754"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
