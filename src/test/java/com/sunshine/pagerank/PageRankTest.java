package com.sunshine.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankTest {

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {
		// 存储网页ID
		private IntWritable id;
		// 存储网页PR值
		private String pr;
		// 存储网页向外链接总数目
		private int count;
		// 网页向每个外部链接的平均贡献值
		private float average_pr;

		public void map(Object key, Text value, Context context) {
			StringTokenizer str = new StringTokenizer(value.toString());
			if (str.hasMoreTokens()) {
				// 得到网页ID
				id = new IntWritable(Integer.parseInt(str.nextToken()));
			} else {
				return;
			}
			// 得到网页pr
			pr = str.nextToken();
			// 得到向外链接数目
			count = str.countTokens();
			// 对每个外部链接平均贡献值
			average_pr = Float.parseFloat(pr) / count;
			// 得到网页的向外链接ID并输出
			while (str.hasMoreTokens()) {
				try {
					String nextId = str.nextToken();
					// 将网页向外链接的ID以“@+得到贡献值”格式输出
					Text t = new Text("@" + average_pr);
					context.write(id, t);
					// 将网页ID和PR值输出
					Text tt = new Text("&" + nextId);
					context.write(id, tt);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) {

			// 定义一个存储网页链接ID的队列
			ArrayList<String> ids = new ArrayList<String>();
			// 将所有的链接ID以String格式保存
			String lianjie = "  ";
			// 定义一个保存网页PR值的变量
			float pr = 0;
			// 遍历
			for (Text id : values) {
				String idd = id.toString();
				// 判断value是贡献值还是向外部的链接
				if (idd.substring(0, 1).equals("@")) {
					// 贡献值
					pr += Float.parseFloat(idd.substring(1));
				} else if (idd.substring(0, 1).equals("&")) {
					// 链接id
					String iddd = idd.substring(1);
					System.out.println("idddd= " + iddd);
					ids.add(iddd);
				}
			}
			// 计算最终pr
			pr = pr * 0.85f + 0.15f;
			// 得到所有链接ID的String形式
			for (int i = 0; i < ids.size(); i++) {
				lianjie += ids.get(i) + "  ";
			}
			// 组合pr+lianjie成原文件的格式类型
			String result = pr + lianjie;
			System.out.println("Reduce    result=" + result);
			try {
				context.write(key, new Text(result));
				System.out.println("reduce 执行完毕。。。。。");
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		String pathIn1 = "/input/pageranktest.txt";// 输入路径
		String pathOut = "/output/201306151158";// 输出路径
		// 迭代10次
		for (int i = 0; i < 10; i++) {
			System.out.println("xunhuan cishu=" + i);
			Job job = new Job(conf, "MapReduce pagerank");
			pathOut = "/output/20130615" + i;
			job.setJarByClass(PageRankTest.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(pathIn1));
			FileOutputFormat.setOutputPath(job, new Path(pathOut));
			pathIn1 = pathOut;
			job.waitForCompletion(true);

		}

	}
}
