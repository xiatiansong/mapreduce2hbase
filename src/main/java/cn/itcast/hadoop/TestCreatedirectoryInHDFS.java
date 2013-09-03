package cn.itcast.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 在hdfs中创建目录
 *
 */
public class TestCreatedirectoryInHDFS {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		hdfs.mkdirs(new Path("/test/a/b/c"));
		
		hdfs.close();
	}
}
