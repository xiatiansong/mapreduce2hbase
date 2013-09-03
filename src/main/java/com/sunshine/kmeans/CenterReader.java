package com.sunshine.kmeans;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

public class CenterReader {

	public Path centerPath;
	private Configuration conf;
	private List<Movie> centers;

	public CenterReader(String centerPath) {
		this.centerPath = new Path(centerPath);
		conf = new Configuration();
		centers = new ArrayList<Movie>();
	}

	public List<Movie> retrieveAllCenters() {
		try {
			_retrieveAllCenters();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return centers;
	}

	private void _retrieveAllCenters() throws IOException {
		FileSystem fs = centerPath.getFileSystem(conf);
		FileStatus[] files = fs.listStatus(centerPath, new CenterPathFilter());
		for (FileStatus file : files) {
			Reader reader = new Reader(fs, file.getPath(), new Configuration());
			Text key = new Text();
			Movie value = new Movie();
			while (reader.next(key, value)) {
				centers.add(value);
				value = new Movie();
			}
			reader.close();
		}
	}

	private static class CenterPathFilter implements PathFilter {

		public boolean accept(Path path) {
			return path.getName().startsWith("part");
		}

	}

}
