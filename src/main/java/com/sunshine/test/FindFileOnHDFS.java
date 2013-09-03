package com.sunshine.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class FindFileOnHDFS {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		getHDFSNodes();
		getFileLocal();
	}

	
	public static void getHDFSNodes() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 强转成分布式文件对象
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		//获取所有节点
		DatanodeInfo[]  nodes = hdfs.getDataNodeStats();
		//循环
		for (int i = 0; i < nodes.length; i++) {
			System.out.println("DataNodes_"+i+"_Name:"+nodes[i].getHostName());
		}
	}
	
	//查找某个文件在集群中的位置
	public static void getFileLocal() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/home/hadoop/data/mk/index.pdf");
		FileStatus fileStatus = fs.getFileStatus(path);
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		int length =  blockLocations.length;
		for (int i = 0; i < length; i++) {
			String[] hosts = blockLocations[i].getHosts();
			System.out.println("block_"+i+"_location:"+hosts[0]);
		}
	}
}
