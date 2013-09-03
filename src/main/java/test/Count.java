package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Count extends Configured implements Tool {
	public static class M extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, new LongWritable(1));
		}
	}

	public static class R extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0;

			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				count += it.next().get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static class C4Count extends MyComparator {
		@Override
		public int compare(Text o1, Text o2) {
			String s1 = o1.toString().split(" ")[0];
			String s2 = o2.toString().split(" ")[0];
			int cmp = s1.compareTo(s2);
			if (cmp != 0)
				return cmp;
			s1 = o1.toString().split(" ")[1];
			s2 = o2.toString().split(" ")[1];
			cmp = s1.compareTo(s2);
			return cmp;
		}
	}

	public static class P extends Partitioner<Text, LongWritable> {
		@Override
		public int getPartition(Text k, LongWritable v, int parts) {
			String[] tmp = k.toString().split(" ");
			int hash = tmp[0].hashCode();

			return (hash & Integer.MAX_VALUE) % parts;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Test Job: Count");
		job.setJarByClass(SortAndCountApp.class);

		job.setMapperClass(M.class);
		job.setReducerClass(R.class);
		job.setNumReduceTasks(3);

		job.setPartitionerClass(P.class);
		job.setSortComparatorClass(C4Count.class);

		FileInputFormat.addInputPaths(job, args[0]);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean successful = job.waitForCompletion(true);

		System.out.println(job.getJobName()
				+ (successful ? " :successful" : " :failed"));

		return successful ? 0 : 1;
	}

}
