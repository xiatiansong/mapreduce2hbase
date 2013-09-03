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
		//step1 �õ�Configuration����
		Configuration con = new Configuration();
        //step2  �õ�FileSystem����
       FileSystem fs = FileSystem.get(con);
        //step3 �����ļ�����
       Path path = new Path("/home/hadoop/data/mk");
       //fs.mkdirs(path);
       
       FileStatus fileStatus = fs.getFileStatus(path);
       System.out.println(fileStatus.getBlockSize());
       System.out.println("Group:"+fileStatus.getGroup()+ "\t Owner:"+fileStatus.getOwner());
	}

}
