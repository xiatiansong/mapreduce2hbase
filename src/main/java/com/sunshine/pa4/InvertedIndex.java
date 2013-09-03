/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sunshine.pa4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author Ming
 */
public class InvertedIndex {

    public static class TokenizerMapper
	    extends Mapper<Text, ValuePair, Text, ValuePair> {

	@Override
	public void map(Text key, ValuePair value, Context context) throws IOException, InterruptedException {
	// TokenInputFormat has generate (word, (fileID, wordPosition))
	// so mapper just spill it to reducer
	    key.set(key.toString().toLowerCase());
	    context.write(key, value);
	}
    }

    public static class IndexReducer
	    extends Reducer<Text, ValuePair, Text, Text> {

	private Text postings = new Text();

	@Override
	public void reduce(Text key, Iterable<ValuePair> values,
		Context context) throws IOException, InterruptedException {
	    String list = "";

	    for (ValuePair val : values) {
		list += " " + val.toString();
	    }
	    postings.set(list);
	    context.write(key, postings);
	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2) {
	    System.err.println("Usage: InvertedIndex <in-dir> <out-dir>");
	    System.exit(2);
	}

	// remove the old output dir
	FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

	Job job = new Job(conf, "Inverted Indexer");
	job.setJarByClass(InvertedIndex.class);
	job.setInputFormatClass(TokenInputFormat.class);
	job.setMapperClass(InvertedIndex.TokenizerMapper.class);
	//job.setCombinerClass(InvertedIndex.IndexReducer.class);
	job.setReducerClass(InvertedIndex.IndexReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(ValuePair.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
