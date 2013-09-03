package com.sunshine.mr2hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseStationCli extends Configured implements Tool {
	static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
	static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
	static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
	static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

	public Map<String, String> getStationInfo(HTable table, String stationId) throws IOException {
		Get get = new Get(Bytes.toBytes(stationId));
		get.addFamily(INFO_COLUMNFAMILY);
		Result res = table.get(get);
		if (res == null) {
			return null;
		}
		Map<String, String> resultMap = new HashMap<String, String>();
		resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
		resultMap.put("location", getValue(res, INFO_COLUMNFAMILY, LOCATION_QUALIFIER));
		resultMap.put("description", getValue(res, INFO_COLUMNFAMILY, DESCRIPTION_QUALIFIER));
		return resultMap;
	}

	private static String getValue(Result res, byte[] cf, byte[] qualifier) {
		byte[] value = res.getValue(cf, qualifier);
		return value == null ? "" : Bytes.toString(value);
	}

	public int run(String[] args) throws IOException {
		/*
		 * if (args.length != 1) {
		 * System.err.println("Usage: HBaseStationCli <station_id>"); return -1;
		 * }
		 */
		JobConf jc = new JobConf(getConf(), getClass());
		jc.set("mapred.job.tracker", "master:9001");
		jc.set("hbase.zookeeper.quorum", "master");
		jc.set("hbase.zookeeper.property.clientPort", "2181");

		HTable table = new HTable(HBaseConfiguration.create(jc), "Stations");
		Map<String, String> stationInfo = getStationInfo(table, "011090-99999");
		if (stationInfo == null) {
			System.err.printf("Station ID %s not found.\n", args[0]);
			return -1;
		}
		for (Map.Entry<String, String> station : stationInfo.entrySet()) {
			// Print the date, time, and temperature
			System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseStationCli(), args);
		System.exit(exitCode);
	}
}