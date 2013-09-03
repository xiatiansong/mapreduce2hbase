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
 * @author ����ǿ
 * @email larry.lv.word@gmail.com
 * @version ����ʱ�䣺2012-5-21 ����5:06:57
 */
public class SecondarySort {
	// map�׶ε����������map��List���з�����ÿ������ӳ�䵽һ��reducer
	public static class FirstPartitioner extends HashPartitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return (key.toString().split(":")[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	// ÿ���������ֵ���job.setSortComparatorClass����key�ıȽϺ�����������
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

	// ֻҪ����Ƚ����Ƚϵ�����key��ͬ�����Ǿ�����ͬһ����.
	// ���ǵ�value����һ��value���������������������keyʹ������ͬһ���������key�ĵ�һ��key
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

	// �Զ���map
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable intvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, intvalue);
		}
	}

	// �Զ���reduce
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void setup(Context context) {
			context.getConfiguration();
			System.out.println("reduce");
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(new Text("-------------------------"), new IntWritable(1));
			for (IntWritable val : values) {
				// ��Ȼ����ͬһ���������ѭ����ÿ�������key������ͬ(key����ȥ�Ǹ�Text��ʵ��Ҳ��һ��list)
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
		// ��������
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(SortComparator.class);
		// ���麯��
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