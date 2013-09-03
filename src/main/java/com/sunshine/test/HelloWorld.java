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
 * mapper -- ���� ��reducer -- ���㡢 job�������������������
 * 
 * @author hadoop
 * 
 */
public class HelloWorld {
	// ���ദ��
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		// ���� ��д��ȥһ����
		private static final IntWritable ONE = new IntWritable(1);
		// �̶�ֵ
		private Text word = new Text("Hello World!");

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(word, ONE);
		}

	}
	
	//reducer��������
	public static class MyReducer extends Reducer< Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException,InterruptedException {
			//����Ϊ1
			result.set(1);
			context.write(key, result);
		}
	}
	
	//����jobtracker
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration cfg = new Configuration();
		//�������������  tasktracker
		Job job = new Job(cfg, "Demo");
		//���ù�����  ������
		job.setJarByClass(HelloWorld.class);
		//����mapper��
		job.setMapperClass(MyMapper.class);
		//����reducer��
		job.setCombinerClass(MyReducer.class);
		//����reducer��
		job.setReducerClass(MyReducer.class);
		//�������
		job.setOutputKeyClass(Text.class);
		//�������ֵ
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/hadoop/fantasy/a.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/hadoop/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
