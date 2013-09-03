package com.sunshine.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition extends Partitioner<TextPair, IntWritable> {

	@Override
	public int getPartition(TextPair key, IntWritable value, int numPartitions) {
		//numPartitions��reduce�ĸ���
		return (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
