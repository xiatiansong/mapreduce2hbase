package com.sunshine.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperReducerTest {

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		MapperClass mapper = new MapperClass();
		ReducerClass reducer = new ReducerClass();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}// end setUp()

	@Test
	public void testMapper() {
		String line = "Google coorperates with IBM in cloud area";

		mapDriver.withInput(new Object(), new Text(line));
		mapDriver.withOutput(new Text("Google"), new IntWritable(1)).withOutput(new Text("coorperates"), new IntWritable(1))
				.withOutput(new Text("with"), new IntWritable(1)).withOutput(new Text("IBM"), new IntWritable(1))
				.withOutput(new Text("in"), new IntWritable(1)).withOutput(new Text("cloud"), new IntWritable(1))
				.withOutput(new Text("area"), new IntWritable(1));

		mapDriver.runTest();
	}// end testMapper()

	@Test
	public void testReducer() {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("Google"), values);
		reduceDriver.withOutput(new Text("Google"), new IntWritable(2));

		reduceDriver.runTest();
	}// end testReducer()

	@Test
	public void testMapperReducer() throws IOException {
		String line = "Google uses Map Reduce Model";
		List<Pair<Text, IntWritable>> out = null;
		List<Pair> expected = new ArrayList<Pair>();

		mapReduceDriver.withInput(new Object(), new Text(line));
		// mapReduceDriver.withOutput(new Text("Google"), new IntWritable(1));
		// mapReduceDriver.withOutput(new Text("uses"), new IntWritable(1));
		// mapReduceDriver.withOutput(new Text("Map"), new IntWritable(1));
		// mapReduceDriver.withOutput(new Text("Reduce"), new IntWritable(1));
		// mapReduceDriver.withOutput(new Text("Model"), new IntWritable(1));
		// mapReduceDriver.runTest();
		out = mapReduceDriver.run();
		
		//mapreduce运行的记过是按a-z的顺序排的
		expected.add(new Pair(new Text("Google"), new IntWritable(1)));
		expected.add(new Pair(new Text("Map"), new IntWritable(1)));
		expected.add(new Pair(new Text("Model"), new IntWritable(1)));
		expected.add(new Pair(new Text("Reduce"), new IntWritable(1)));
		expected.add(new Pair(new Text("uses"), new IntWritable(1)));

		Assert.assertEquals(expected, out);
	}// end testMapperReducer()
}
