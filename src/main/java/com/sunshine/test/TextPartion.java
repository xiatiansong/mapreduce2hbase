package com.sunshine.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPartion extends  Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		int result = 0;
		if(key.equals("long")){
			result = 0 % numPartitions;
		}else if(key.equals("short")){
			result = 1 % numPartitions;
		}else if(key.equals("right")){
			result = 2 % numPartitions;
		}
		return result;
	}

}
