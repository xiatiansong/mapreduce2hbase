package com.sunshine.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sunshine.test.HelloWorld;
import com.sunshine.test.HelloWorld.MyMapper;
import com.sunshine.test.HelloWorld.MyReducer;

public class SortMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration cfg = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(cfg, args).getRemainingArgs();
//	    if (otherArgs.length != 2) {
//	      System.err.println("Usage: wordcount <in> <out>");
//	      System.exit(2);
//	    } 
		// 设置任务的名称 tasktracker
		Job job = new Job(cfg, "Sort");
		// 设置工作类 即本类
		job.setJarByClass(SortMain.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// 设置mapper类
		job.setMapperClass(SortMapper.class);
		// 设置reducer类
		job.setReducerClass(SortReducer.class);
		// 设置输出
		job.setMapOutputKeyClass(IntPaire.class);
		// 设置输出值
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(TextIntComparator.class);
		job.setGroupingComparatorClass(TextComparator.class);
		job.setPartitionerClass(PartitionByText.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/hadoop/data/mk/mapred_02.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/output2"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
