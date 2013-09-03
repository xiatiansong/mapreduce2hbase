package com.sunshine.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, IntWritable> {

	public enum COUNTER_KEYS {
		INVALID_LINES, NOT_LISTEN
	};
	private final static IntWritable one = new IntWritable(1);
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
