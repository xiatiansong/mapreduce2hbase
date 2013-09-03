package com.sunshine.test;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

public class MyMultiOutputFormat extends MultipleOutputFormat<Text, IntWritable> {

	private TextOutputFormat<Text, IntWritable> output = null;

	@Override
	protected RecordWriter<Text, IntWritable> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable process) throws IOException {
		if (output == null) {
			output = new TextOutputFormat<Text, IntWritable>();
		}
		return output.getRecordWriter(fs, job, name, process);
	}

	@Override
	protected String generateFileNameForKeyValue(Text key, IntWritable value, String name) {
		char c = key.toString().toLowerCase().charAt(0);
		if (c >= 'a' && c <= 'z') {
			return c + ".txt";
		}
		return "result.txt";
	}

}
