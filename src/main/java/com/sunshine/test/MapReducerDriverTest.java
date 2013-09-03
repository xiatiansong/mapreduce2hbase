package com.sunshine.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sunshine.mapred.MapperClass;
import com.sunshine.mapred.ReducerClass;

public class MapReducerDriverTest {
	
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> driver = null;
	private MapReduceDriver<Object, Text, Text, IntWritable,Text, IntWritable> mapreduceDriver = null;
	private List<Pair<Text, IntWritable>> run;
	
	@Before
	public void init() {
		MapperClass mapper = new MapperClass();
		ReducerClass reduce = new ReducerClass();
		mapDriver = MapDriver.newMapDriver(mapper);
		driver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reduce);
		mapreduceDriver = new MapReduceDriver<Object, Text, Text, IntWritable,Text, IntWritable>(mapper, reduce);
	}
	
	@Test
	public void testMap() throws IOException {
		String line = "this is a test case for map driver";
		mapDriver.withInput(new Object(), new Text(line)).withOutput(new Text("this"), new IntWritable(1))
				.withOutput(new Text("is"), new IntWritable(1)).withOutput(new Text("a"), new IntWritable(1))
				.withOutput(new Text("test"), new IntWritable(1)).withOutput(new Text("case"), new IntWritable(1))
				.withOutput(new Text("for"), new IntWritable(1)).withOutput(new Text("map"), new IntWritable(1))
				.withOutput(new Text("driver"), new IntWritable(1)).runTest();
	}
	
	@Test
	public void testReduce(){
		//int[] values = new int[]{2,3,4,5,1};
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		values.add(new IntWritable(3));
		driver.withInput(new Text("taobao"),values)
				 .withOutput(new Text("taobao"),new IntWritable(6))
				 .runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException{
		String line = "this is a test case for map reduce this is a new test case";
		run = mapreduceDriver.withInput(new Pair<Object, Text>(new Object(), new Text(line)))
								 .withOutput(new Text("this"), new IntWritable(2))
								 .withOutput(new Text("is"), new IntWritable(2))
								 .withOutput(new Text("a"), new IntWritable(2))
								 .withOutput(new Text("test"), new IntWritable(2))
								 .withOutput(new Text("case"), new IntWritable(2))
								 .withOutput(new Text("for"), new IntWritable(1))
								 .withOutput(new Text("map"), new IntWritable(2))
								 .withOutput(new Text("reduce"), new IntWritable(2))
								 .run();
		Assert.assertEquals(new IntWritable(2), run.get(0).getSecond());
		Counters counters  = mapreduceDriver.getCounters();
		Assert.assertEquals(0L,counters.findCounter(MapperClass.COUNTER_KEYS.INVALID_LINES).getValue());
	}
}
