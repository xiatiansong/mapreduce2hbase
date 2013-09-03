package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortAndCount extends Configured implements Tool {

	public static class M extends Mapper<LongWritable, Text, K, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new K(value.toString()), new LongWritable(1));
		}
	}

	public static class R extends Reducer<K, LongWritable, K, LongWritable> {
		static int times = 0;

		@Override
		protected void reduce(K key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0;

			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				count += it.next().get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static class P extends Partitioner<K, LongWritable> {
		@Override
		public int getPartition(K k, LongWritable v, int parts) {
			int hash = k.getA().hashCode();

			return (hash & Integer.MAX_VALUE) % parts;
		}
	}

	public static class C implements RawComparator<K> {
		@Override
		public int compare(K o1, K o2) {
			String s1 = o1.getA();
			String s2 = o2.getA();
			int cmp = s1.compareTo(s2);
			if (cmp != 0)
				return cmp;
			s1 = o1.getB();
			s2 = o2.getB();
			cmp = s1.compareTo(s2);
			return cmp;
		}

		@Override
		public final int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
				int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);

			byte[] _b1 = Arrays.copyOfRange(b1, s1 + n1, s1 + l1);
			byte[] _b2 = Arrays.copyOfRange(b2, s2 + n2, s2 + l2);

			String t1 = new String(_b1);
			String t2 = new String(_b2);

			return compare(new K(t1), new K(t2));
		}
	}

	public static class K implements WritableComparable<K> {

		private String a;
		private String b;
		private String c;

		public K() {
		}

		public K(String key) {
			String[] tmp = key.split(" ");
			this.a = tmp[0];
			this.b = tmp[1];
			this.c = tmp[2];
		}

		public String getA() {
			return a;
		}

		public void setA(String a) {
			this.a = a;
		}

		public String getB() {
			return b;
		}

		public void setB(String b) {
			this.b = b;
		}

		public String getC() {
			return c;
		}

		public void setC(String c) {
			this.c = c;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((a == null) ? 0 : a.hashCode());
			result = prime * result + ((b == null) ? 0 : b.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof K))
				return false;
			K other = (K) o;
			boolean eq = this.getA().equals(other.getA());
			if (eq) {
				eq = this.getB().equals(other.getB());
			}
			return eq;
		}

		@Override
		public String toString() {
			String[] tmp = { a, b, c };
			return StringUtils.join(tmp, " ");
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			String[] tmp = in.readUTF().split(" ");
			a = tmp[0];
			b = tmp[1];
			c = tmp[2];
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(toString());
		}

		@Override
		public int compareTo(K o) {
			int cmp = this.getA().compareTo(o.getA());
			if (cmp != 0)
				return cmp;
			cmp = this.getB().compareTo(o.getB());
			if (cmp != 0)
				return cmp;
			cmp = this.getC().compareTo(o.getC());
			return cmp;
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Test Job: Sort and Count");
		job.setJarByClass(SortAndCount.class);

		job.setMapperClass(M.class);
		job.setReducerClass(R.class);
		job.setNumReduceTasks(3);

		job.setPartitionerClass(P.class);
		// job.setSortComparatorClass(InnerC.class);
		job.setGroupingComparatorClass(C.class);

		FileInputFormat.addInputPaths(job, args[0]);
		job.setOutputKeyClass(K.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean successful = job.waitForCompletion(true);

		System.out.println(job.getJobName()
				+ (successful ? " :successful" : " :failed"));

		return successful ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");
		System.exit(ToolRunner.run(new Configuration(), new SortAndCount(),
				args));
	}

}
