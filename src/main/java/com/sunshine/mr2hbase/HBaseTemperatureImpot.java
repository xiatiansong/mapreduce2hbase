package com.sunshine.mr2hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HBaseTemperatureImpot {
	public static void createHBaseTable(String tableName) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(tableName);

		HColumnDescriptor col = new HColumnDescriptor("data");
		htd.addFamily(col);

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists(tableName)) {
			System.out.println("该数据表已经存在，正在重新创建。");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}
		System.out.println("创建表：" + tableName);
		hAdmin.createTable(htd);
	}

	public static void main(String[] args) throws Exception {
		String tableName = "Observations";

		createHBaseTable(tableName);

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		conf.set("hbase.zookeeper.quorum", "master");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.mapred.outputtable", tableName);

		Job job = new Job(conf, "HBaseTemperatureImpot");
		job.setJarByClass(HBaseTemperatureImpot.class);

		job.setMapperClass(TemperatureMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("/input/ncdc/all"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TemperatureMapper<V, K> extends Mapper<LongWritable, Text, K, V> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		private HTable table;

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, K, V>.Context context) throws IOException, InterruptedException {
			this.parser.parse(value.toString());
			if (this.parser.isValidTemperature()) {
				byte[] rowKey = RowKeyConverter.makeObservationRowKey(this.parser.getStationId(), this.parser.getObservationDate().getTime());
				Put p = new Put(rowKey);
				p.add(HBaseTemperatureCli.DATA_COLUMNFAMILY, HBaseTemperatureCli.AIRTEMP_QUALIFIER, Bytes.toBytes(this.parser.getAirTemperature()));
				this.table.put(p);
			}
		}

		protected void cleanup(Mapper<LongWritable, Text, K, V>.Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			this.table.close();
		}

		protected void setup(Mapper<LongWritable, Text, K, V>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.table = new HTable(HBaseConfiguration.create(), "Observations");
		}
	}
}