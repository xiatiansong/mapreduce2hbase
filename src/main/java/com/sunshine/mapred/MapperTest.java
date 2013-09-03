package com.sunshine.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class MapperTest {
	
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void init(){
		MapperClass mapper = new MapperClass();
		mapDriver = MapDriver.newMapDriver(mapper);
	}
	
	@Test
	public void test() throws IOException{
		String line = "this is a test case for map driver";
		mapDriver.withInput(new Object(), new Text(line))
				     .withOutput(new Text("this"), new IntWritable(1))
				     .withOutput(new Text("is"), new IntWritable(1))
				     .withOutput(new Text("a"), new IntWritable(1))
				     .withOutput(new Text("test"), new IntWritable(1))
				     .withOutput(new Text("case"), new IntWritable(1))
				     .withOutput(new Text("for"), new IntWritable(1))
				     .withOutput(new Text("map"), new IntWritable(1))
				     .withOutput(new Text("driver"), new IntWritable(1)).runTest();
	}
	
}
