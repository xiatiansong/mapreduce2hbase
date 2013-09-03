package com.sunshine.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Progressable;

public class MultiWordCount {
	
	public static  class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
		private final  IntWritable one = new IntWritable(1);
	    private Text keyText = new Text("key"); 
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//ªÒ»°÷µ
			String str = value.toString();
			StringTokenizer st = new  StringTokenizer(str);
			while (st.hasMoreElements()) {
				keyText.set(st.nextToken());
				context.write(keyText, one);
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class AlphabetOutPutFormat extends MultipleOutputFormat<Text, IntWritable>{
		@Override
		protected RecordWriter<Text, IntWritable> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
