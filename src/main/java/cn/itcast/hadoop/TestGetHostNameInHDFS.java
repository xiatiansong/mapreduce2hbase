package cn.itcast.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * 获取 HDFS集群上所有节点名称
 * 
 */
public class TestGetHostNameInHDFS {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 强转成分布式文件对象
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		// 获取节点信息－数组
		DatanodeInfo[] dis = hdfs.getDataNodeStats();
		for (DatanodeInfo info : dis) {
			String name = info.getHostName();
			System.err.println(name);
		}
	}

}
