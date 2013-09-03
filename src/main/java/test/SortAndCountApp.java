package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class SortAndCountApp {

	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");

		String tmpPath = args[1] + "_tmp";
		int code = ToolRunner.run(new Configuration(), new Sort(),
				new String[] { args[0], tmpPath });
		if (code < 1) {
			ToolRunner.run(new Configuration(), new Count(), new String[] {
					tmpPath, args[1] });
		}
	}

}
