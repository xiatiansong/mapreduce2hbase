package com.sunshine.pagerank.builder;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Reduce extends MapReduceBase 
	implements Reducer<WritableComparable, Writable, WritableComparable, Text> {

	public void reduce(WritableComparable key, Iterator<Writable> values, 
			OutputCollector<WritableComparable, Text> output, Reporter reporter) throws IOException {
		//reporter.setStatus(_key.toString());
		Text title = (Text) key;
		String toWrite = "";
		int count = 0;
		while (values.hasNext()) {
			String page = ((Text)values.next()).toString();
			//page.replaceAll(" ", "_");
			toWrite += " " + page;
			count += 1;
		}

		String num = (new Integer(count)).toString();
		toWrite = num + "@@@" + toWrite;
		output.collect(title, new Text(toWrite));
	}




}
