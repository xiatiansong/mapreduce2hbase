package com.sunshine.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * mapper -- 分类 、reducer -- 计算、 job来调用上面的两个功能
 * 
 * @author hadoop
 * 
 */
public class HelloWorld {
	// 分类处理
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		// 声明 先写出去一个数
		private static final IntWritable ONE = new IntWritable(1);
		// 固定值
		private Text word = new Text("Hello World!");

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(word, ONE);
		}

	}
	
	//reducer处理数据
	public static class MyReducer extends Reducer< Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException,InterruptedException {
			//设置为1
			result.set(1);
			context.write(key, result);
		}
	}
	
	//生命jobtracker
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration cfg = new Configuration();
		//设置任务的名称  tasktracker
		Job job = new Job(cfg, "Demo");
		//设置工作类  即本类
		job.setJarByClass(HelloWorld.class);
		//设置mapper类
		job.setMapperClass(MyMapper.class);
		//设置reducer类
		job.setCombinerClass(MyReducer.class);
		//设置reducer类
		job.setReducerClass(MyReducer.class);
		//设置输出
		job.setOutputKeyClass(Text.class);
		//设置输出值
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/hadoop/fantasy/a.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/hadoop/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
