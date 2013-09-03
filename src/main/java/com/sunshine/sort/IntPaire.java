package com.sunshine.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * map输出的自定义序列
 * @author hadoop
 *
 */
public class IntPaire implements WritableComparable<IntPaire> {

	private String firstKey;
	private int secondKey;

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public int getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(int secondKey) {
		this.secondKey = secondKey;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstKey);
		out.writeInt(secondKey);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstKey = in.readUTF();
		secondKey = in.readInt();
	}

	@Override
	public int compareTo(IntPaire o) {
		return this.firstKey.compareTo(o.getFirstKey());
	}
	
}
