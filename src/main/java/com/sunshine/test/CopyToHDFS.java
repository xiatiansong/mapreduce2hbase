package com.sunshine.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToHDFS {

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
       Path source = new Path("e:"+File.separator+"mapred_02.txt");
       Path dist = new Path("/data/ch04/");
       fs.copyFromLocalFile(source, dist);
	}
}
