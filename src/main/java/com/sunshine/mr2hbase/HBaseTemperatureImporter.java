package com.sunshine.mr2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.*;

public class HBaseTemperatureImporter extends Configured implements Tool {
	
	// Inner-class for map
	static class HBaseTemperatureMapper<K, V> extends MapReduceBase implements Mapper<LongWritable, Text, K, V> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		private HTable table;

		public void map(LongWritable key, Text value, OutputCollector<K, V> output, Reporter reporter) throws IOException {
			parser.parse(value.toString());
			if (parser.isValidTemperature()) {
				byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(), parser.getObservationDate().getTime());
				Put p = new Put(rowKey);
				p.add(HBaseTemperatureCli.DATA_COLUMNFAMILY, HBaseTemperatureCli.AIRTEMP_QUALIFIER, Bytes.toBytes(parser.getAirTemperature()));
				table.put(p);
			}
		}

		public void configure(JobConf jc) {
			super.configure(jc);
			// Create the HBase table client once up-front and keep it around
			// rather than create on each map invocation.
			try {
				this.table = new HTable(HBaseConfiguration.create(jc), "Observations");
			} catch (IOException e) {
				throw new RuntimeException("Failed HTable construction", e);
			}
		}

		@Override
		public void close() throws IOException {
			super.close();
			table.close();
		}
	}

	public int run(String[] args) throws IOException {
		/*if (args.length != 1) {
			System.err.println("Usage: HBaseTemperatureImporter <input>");
			return -1;
		}*/
		JobConf jc = new JobConf(getConf(), getClass());
		jc.set("mapred.job.tracker", "master:9001");
		FileInputFormat.addInputPath(jc, new Path("/input/ncdc/all"));
		jc.setMapperClass(HBaseTemperatureMapper.class);
		jc.setNumReduceTasks(0);
		jc.setOutputFormat(NullOutputFormat.class);
		JobClient.runJob(jc);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseTemperatureImporter(), args);
		System.exit(exitCode);
	}
}