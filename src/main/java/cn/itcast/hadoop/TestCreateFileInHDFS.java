package cn.itcast.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 创建hdfs文件
 *
 */
public class TestCreateFileInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		FSDataOutputStream out = hdfs.create(new Path("/test/abc"));
		
		out.write("hello hadoop".getBytes());
		
		out.close();
		
		hdfs.close();
	}
}
