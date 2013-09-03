package com.sunshine.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonReduce extends Reducer<TextPair, IntWritable, Text, Text> {

	@Override
	protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		//first value is tradeId
		String tradeId = values.iterator().next().toString();
		while(values.iterator().hasNext()){
			//next value is payId
			String payId = values.iterator().next().toString();
			context.write(new Text(tradeId), new Text(payId));
		}
	}

}
