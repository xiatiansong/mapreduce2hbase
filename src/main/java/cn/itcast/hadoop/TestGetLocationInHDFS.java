package cn.itcast.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 查找某个文件在HDFS集群中的位置
 * 
 */
public class TestGetLocationInHDFS {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 必须是一个具体的文件
		Path path = new Path("/test/wordcount.txt");
		// 文件状态
		FileStatus fileStatus = fs.getFileStatus(path);
		// 文件块
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus,
				0, fileStatus.getLen());
		int blockLen = blockLocations.length;
		//System.err.println(blockLen);
		for (int i = 0; i < blockLen; i++) {
			// 主机名
			String[] hosts = blockLocations[i].getHosts();
			for (String host : hosts) {
				System.err.println(host);
			}
		}
	}

}
