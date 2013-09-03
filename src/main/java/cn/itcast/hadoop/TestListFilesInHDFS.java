package cn.itcast.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 列出hdfs文件
 *
 */
public class TestListFilesInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		FileStatus[] fs = hdfs.listStatus(new Path("/home/hadoop/data/mk/"));
		
		for(FileStatus f: fs){
			System.out.println("当前文件：" + f.getPath());
		}
		
		hdfs.close();
	}
}
