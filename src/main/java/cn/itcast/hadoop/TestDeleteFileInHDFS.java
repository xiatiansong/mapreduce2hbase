package cn.itcast.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 删除hdfs文件或目录
 *
 */
public class TestDeleteFileInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		//参数2表示递归删除，如果删除的为目录，此参数要设置文true
		hdfs.delete(new Path("/test/a"), true);
		
		hdfs.close();
	}
}
