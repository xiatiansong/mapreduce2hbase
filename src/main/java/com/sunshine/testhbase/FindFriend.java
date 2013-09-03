package com.sunshine.testhbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class FindFriend {
	/**
	 * mapper
	 * 
	 * @author hadoop
	 * 
	 */
	public static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		public Mapper() {
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			ImmutableBytesWritable value = null;
			String[] tags = null;
			for (KeyValue kv : values.list()) {
				if ("author".equals(Bytes.toString(kv.getFamily())) && "nickname".equals(Bytes.toString(kv.getQualifier()))) {
					value = new ImmutableBytesWritable(kv.getValue());
				}
				if ("article".equals(Bytes.toString(kv.getFamily())) && "tags".equals(Bytes.toString(kv.getQualifier()))) {
					tags = Bytes.toString(kv.getValue()).split(",");
				}
			}
			for (int i = 0; i < tags.length; i++) {
				ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(tags[i].toLowerCase()));
				try {
					context.write(key, value);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		}
	}

	/**
	 * reducer
	 * 
	 * @author hadoop
	 * 
	 */
	public static class Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException,
				InterruptedException {
			String friends = "";
			for (ImmutableBytesWritable val : values) {
				friends += (friends.length() > 0 ? "," : "") + Bytes.toString(val.get());
			}
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("person"), Bytes.toBytes("nicknames"), Bytes.toBytes(friends));
			context.write(key, put);
		}
	}

	// 在提交作业时设置 inputFormat 为 TableInputFormat,设置 outputFormat 为
	//TableOutputFormat，可以借助 TableMapReduceUtil 类来简化编码。
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create(conf);
		Job job = new Job(conf, "HBase_FindFriend");
		job.setJarByClass(FindFriend.class);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("author"), Bytes.toBytes("nickname"));
		scan.addColumn(Bytes.toBytes("article"), Bytes.toBytes("tags"));
		TableMapReduceUtil.initTableMapperJob("blog", scan, FindFriend.Mapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("tag_friend", FindFriend.Reducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}