package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Sort extends Configured implements Tool {
	public static class M extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}

	public static class C4Srot extends MyComparator {
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
			if (cmp != 0)
				return cmp;
			s1 = o1.toString().split(" ")[2];
			s2 = o2.toString().split(" ")[2];
			cmp = s1.compareTo(s2);
			return cmp;
		}
	}
	
	public static class P extends Partitioner<Text, NullWritable> {
		@Override
		public int getPartition(Text k, NullWritable v, int parts) {
			String[] tmp = k.toString().split(" ");
			int hash = tmp[0].hashCode();

			return (hash & Integer.MAX_VALUE) % parts;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Test Job: Sort");
		job.setJarByClass(SortAndCountApp.class);

		job.setMapperClass(M.class);
		job.setNumReduceTasks(3);

		job.setPartitionerClass(P.class);
		job.setSortComparatorClass(C4Srot.class);

		FileInputFormat.addInputPaths(job, args[0]);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean successful = job.waitForCompletion(true);

		System.out.println(job.getJobName()
				+ (successful ? " :successful" : " :failed"));

		return successful ? 0 : 1;
	}

}
