package com.sunshine.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class FileFilter implements Filter{
	
	private byte[] value;
	
	public FileFilter(){}
	
	public FileFilter(String s){this.value = s.getBytes();}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Bytes.writeByteArray(out, this.value);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.value = Bytes.readByteArray(in);
	}

	public void reset() {
		// TODO Auto-generated method stub
		
	}

	public boolean filterRowKey(byte[] buffer, int offset, int length) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean filterAllRemaining() {
		// TODO Auto-generated method stub
		return false;
	}

	public ReturnCode filterKeyValue(KeyValue v) {
		// TODO Auto-generated method stub
		return null;
	}

	public KeyValue transform(KeyValue v) {
		// TODO Auto-generated method stub
		return null;
	}

	public void filterRow(List<KeyValue> kvs) {
		// TODO Auto-generated method stub
		
	}

	public boolean hasFilterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean filterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	public KeyValue getNextKeyHint(KeyValue currentKV) {
		// TODO Auto-generated method stub
		return null;
	}

}
