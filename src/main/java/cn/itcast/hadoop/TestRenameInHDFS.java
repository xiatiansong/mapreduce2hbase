package cn.itcast.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 重命名hdfs文件(移动文件)
 *
 */
public class TestRenameInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		hdfs.rename(new Path("/test/itcast.txt"), new Path("/var/hadoop.txt"));
		
		hdfs.close();
	}
}
