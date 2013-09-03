package com.sunshine.mr2hbase;

import java.io.*;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.*;

public class HBaseStationImporter extends Configured implements Tool {

	public int run(String[] args) throws IOException {
		/*if (args.length != 1) {
			System.err.println("Usage: HBaseStationImporter <input>");
			return -1;
		}*/
		JobConf jc = new JobConf(getConf(), getClass());
		jc.set("mapred.job.tracker", "master:9001");
		jc.set("hbase.zookeeper.quorum", "master");
		jc.set("hbase.zookeeper.property.clientPort", "2181");
		
		HTable table = new HTable(HBaseConfiguration.create(jc), "Stations");

		NcdcStationMetadata metadata = new NcdcStationMetadata();
		metadata.initialize(new File("E://stations-fixed-width.txt"));
		Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

		for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
			Put put = new Put(Bytes.toBytes(entry.getKey()));
			put.add(HBaseStationCli.INFO_COLUMNFAMILY, HBaseStationCli.NAME_QUALIFIER, Bytes.toBytes(entry.getValue()));
			put.add(HBaseStationCli.INFO_COLUMNFAMILY, HBaseStationCli.DESCRIPTION_QUALIFIER, Bytes.toBytes("Description..."));
			put.add(HBaseStationCli.INFO_COLUMNFAMILY, HBaseStationCli.LOCATION_QUALIFIER, Bytes.toBytes("Location..."));
			table.put(put);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseStationImporter(), args);
		System.exit(exitCode);
	}
}