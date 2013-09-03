package com.sunshine.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * @author 吕桂强
 * @email larry.lv.word@gmail.com
 * @version 创建时间：2012-5-21 下午5:06:57
 */
public class SecondarySort {
	// map阶段的最后会对整个map的List进行分区，每个分区映射到一个reducer
	public static class FirstPartitioner extends HashPartitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return (key.toString().split(":")[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	// 每个分区内又调用job.setSortComparatorClass或者key的比较函数进行排序
	public static class SortComparator extends WritableComparator {
		protected SortComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return -w1.toString().split(":")[0].compareTo(w2.toString().split(":")[0]);
		}
	}

	// 只要这个比较器比较的两个key相同，他们就属于同一个组.
	// 它们的value放在一个value迭代器，而这个迭代器的key使用属于同一个组的所有key的第一个key
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return w1.toString().split(":")[0].compareTo(w2.toString().split(":")[0]);
		}
	}

	// 自定义map
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable intvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, intvalue);
		}
	}

	// 自定义reduce
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void setup(Context context) {
			context.getConfiguration();
			System.out.println("reduce");
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(new Text("-------------------------"), new IntWritable(1));
			for (IntWritable val : values) {
				// 虽然分在同一个组里，但是循环里每次输出的key都不相同(key看上去是个Text但实际也是一个list)
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "secondarysort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// 分区函数
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(SortComparator.class);
		// 分组函数
		job.setGroupingComparatorClass(GroupingComparator.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/larry/wc/input"));
		FileOutputFormat.setOutputPath(job, new Path("/larry/wc/output"));

		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}