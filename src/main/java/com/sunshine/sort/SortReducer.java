package com.sunshine.sort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntPaire, IntWritable, Text, Text> {

	@Override
	protected void reduce(IntPaire key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		StringBuffer sb = new StringBuffer();
		 Iterator<IntWritable>itr =  values.iterator();
		while (itr.hasNext()) {
			int value = itr.next().get();
			sb.append(value + ",");
		}
		if(sb.length() > 0){
			//sb = new StringBuffer(sb.substring(0, sb.length() -1));
			sb = sb.deleteCharAt( sb.length() -1);
		}
		context.write(new Text(key.getFirstKey()), new Text(sb.toString()));
	}
}
