package cn.itcast.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class Test {

	public static void main(String[] args) {

		String str = "hello world hadoop itcast";
		String s = "12345";
		System.out.println(s.hashCode());
		System.out.println((s.hashCode() & Integer.MAX_VALUE)%4);
		/*String[] ss = str.split(" ");
		
		for(String s:ss){
			System.out.println(s);
		}*/
		
		
		String word = new String();//Text word = new Text();
		Map<String, Integer> map = new HashMap<String, Integer>();
		StringTokenizer st = new StringTokenizer(str);
		
		while(st.hasMoreTokens()){
			
			String s1 = st.nextToken();
			word = s1;//word.set(s1);
			map.put(word, 1);
			System.out.println(s1);
		}
		
		Configuration conf = new Configuration();
		DistributedCache.createSymlink(conf);
		
	}
	
	@org.junit.Test
	public void  testFS() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path("");
		Path dst = new Path("");
		fs.copyFromLocalFile(src, dst);
	}
	
	
	public class MyPathFilter implements PathFilter{

		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
}
