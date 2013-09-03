package com.sunshine.hadoopaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPaire implements WritableComparable<TextPaire>{

	public void write(DataOutput out) throws IOException {
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int compareTo(TextPaire o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
