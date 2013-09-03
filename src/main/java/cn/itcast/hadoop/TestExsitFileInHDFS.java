package cn.itcast.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 查看某个HDFS文件或目录是否存在
 * 
 */
public class TestExsitFileInHDFS {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);	
		
		boolean b = hdfs.exists(new Path("/test/ddd"));
		System.out.println(b);
		hdfs.close();
	}
	
		
}
