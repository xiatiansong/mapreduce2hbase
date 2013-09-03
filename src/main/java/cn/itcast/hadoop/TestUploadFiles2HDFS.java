package cn.itcast.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 上传本地文件或目录到HDFS
 *
 */
public class TestUploadFiles2HDFS {

	public static void main(String[] args) throws Exception {
		
		//初始化配置对象
		Configuration conf = new Configuration();
		
		//获得hdfs对象
		FileSystem hdfs = FileSystem.get(conf);
		
		//本地文件
		Path p1 = new Path("E:\\input");
		
		//目标文件
		Path p2 = new Path("/");
		
		//完成文件上传
		hdfs.copyFromLocalFile(p1, p2);
		
		System.out.println("文件上传至：" + conf.get("fs.default.name"));
		hdfs.close();
	}

}
