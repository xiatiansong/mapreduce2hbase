package com.sunshine.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSMkdir {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//step1 得到Configuration对象
		Configuration con = new Configuration();
        //step2  得到FileSystem对象
       FileSystem fs = FileSystem.get(con);
        //step3 进行文件操作
       Path path = new Path("/home/hadoop/data/mk");
       //fs.mkdirs(path);
       
       FileStatus fileStatus = fs.getFileStatus(path);
       System.out.println(fileStatus.getBlockSize());
       System.out.println("Group:"+fileStatus.getGroup()+ "\t Owner:"+fileStatus.getOwner());
	}

}
