package com.sunshine.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText extends Partitioner<IntPaire, IntWritable> {

	@Override
	public int getPartition(IntPaire key, IntWritable value, int numPartitions) {
		//numPartitions��reduce�ĸ���
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
