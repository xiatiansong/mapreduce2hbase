package com.sunshine.join;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sunshine.test.HelloWorld;
import com.sunshine.test.HelloWorld.MyMapper;
import com.sunshine.test.HelloWorld.MyReducer;

public class JoinMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		//Hashtable<Integer, String> ht = new Hashtable<Integer, String>();
		Configuration cfg = new Configuration();
		// 设置任务的名称 tasktracker
		Job job = new Job(cfg, "JoinMain");
		// 设置工作类 即本类
		job.setJarByClass(JoinMain.class);
		// 设置mapper类
		job.setMapperClass(PreMapper.class);
		// 设置输出
		job.setMapOutputKeyClass(TextPair.class);
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(FirstComparator.class);
		// 设置输出值
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置reducer类
		job.setReducerClass(CommonReduce.class);

		FileInputFormat.addInputPath(job, new Path("/home/hadoop/data/product/"));
		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/output3"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
