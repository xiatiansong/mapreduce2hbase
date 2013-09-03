package com.sunshine.fmlast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TrackStatisticsProgram {
	public static final Log log = LogFactory.getLog(TrackStatisticsProgram.class);

	// values below indicate position in raw data for each value
	private static final int COL_USERID = 0;
	private static final int COL_TRACKID = 1;
	private static final int COL_SCROBBLES = 2;
	private static final int COL_RADIO = 3;
	private static final int COL_SKIP = 4;

	private Configuration conf;

	/**
	 * Constructs a new TrackStatisticsProgram, using a default Configuration.
	 */
	public TrackStatisticsProgram() {
		this.conf = new Configuration();
	}

	/**
	 * Enumeration for Hadoop error counters.
	 */
	private enum COUNTER_KEYS {
		INVALID_LINES, NOT_LISTEN
	};

	/**
	 * Mapper that takes in raw listening data and outputs the number of unique
	 * listeners per track.
	 */
	public static class UniqueListenersMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		protected void map(LongWritable position, Text rawLine, Context context) throws IOException, InterruptedException {
			String line = (rawLine).toString();
			if (line.trim().isEmpty()) { // if the line is empty, report error  and ignore
				context.getCounter(COUNTER_KEYS.INVALID_LINES).increment(1);
				return;
			}
			String[] parts = line.split(" "); // raw data is whitespace  delimited
			try {
				int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
				int radioListens = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
				//当歌曲的收藏和电台收听都小于或等于0时，词条记录有问题
				if (scrobbles <= 0 && radioListens <= 0) {
					// if track somehow is marked with zero plays, report error  and ignore
					context.getCounter(COUNTER_KEYS.NOT_LISTEN).increment(1);
					return;
				}
				// if we get to here then user has listened to track, so output  user id against track id
				IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]));
				IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_USERID]));
				context.write(trackId, userId);
			} catch (NumberFormatException e) {
				context.getCounter(COUNTER_KEYS.INVALID_LINES).increment(1);
				context.setStatus("Invalid line in listening data: " + rawLine);
				return;
			}
		}
	}
	
	
	/**
	 * Combiner that improves efficiency by removing duplicate user ids from
	 * mapper output.
	 */
	public static class UniqueListenersCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

		@Override
		protected void reduce(IntWritable trackId, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			Set<IntWritable> userIds = new HashSet<IntWritable>();
			Iterator<IntWritable> intValues =  values.iterator();
			while (intValues.hasNext()) {
				IntWritable userId = intValues.next();
				if (!userIds.contains(userId)) {
					// if this user hasn't already been marked as listening to the track, add them to set and output them
					userIds.add(new IntWritable(userId.get()));
					context.write(trackId, userId);
				}
			}
		}
	}
	
	/**
	 * Reducer that outputs only unique listener ids per track (i.e. it removes
	 * any duplicated). Final output is number of unique listeners per track.
	 */
	public static class UniqueListenersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		protected void reduce(IntWritable trackId, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			Iterator<IntWritable> intValues =  values.iterator();
			Set<Integer> userIds = new HashSet<Integer>();
			// add all userIds to the set, duplicates automatically removed (set contract)
			while (intValues.hasNext()) {
				IntWritable userId = intValues.next();
				userIds.add(Integer.valueOf(userId.get()));
			}
			// output trackId -> number of unique listeners per track
			context.write(trackId, new IntWritable(userIds.size()));
		}
	}
	
	/**
	 * Mapper that summarizes various statistics per track. Input is raw
	 * listening data, output is a partially filled in TrackStatistics object
	 * per track id.
	 */
	public static class SumMapper extends Mapper<LongWritable, Text, IntWritable, TrackStats> {

		@Override
		protected void map(LongWritable key, Text rawLine, Context context) throws IOException, InterruptedException {
			String line = (rawLine).toString();
			if (line.trim().isEmpty()) { // ignore empty lines
				context.getCounter(COUNTER_KEYS.INVALID_LINES).increment(1);
				return;
			}
			String[] parts = line.split(" ");
			try {
				int trackId = Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]);
				int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
				int radio = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
				int skip = Integer.parseInt(parts[TrackStatisticsProgram.COL_SKIP]);
				// set number of listeners to 0 (this is calculated later) and other values as provided in text file
				TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
				context.write(new IntWritable(trackId), trackstat);
			} catch (NumberFormatException e) {
				context.getCounter(COUNTER_KEYS.INVALID_LINES).increment(1);
				log.warn("Invalid line in listening data: " + rawLine);
			}
		}
	}
	
	/**
	 * Sum up the track statistics per track. Output is a TrackStatistics object
	 * per track id.
	 */
	public static class SumReducer extends Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

		@Override
		protected void reduce(IntWritable trackId, Iterable<TrackStats> values, Context context) throws IOException,
				InterruptedException {
			Iterator<TrackStats> intValues =  values.iterator();
			TrackStats sum = new TrackStats(); // holds the totals for this
			// track
			while (intValues.hasNext()) {
				TrackStats trackStats = (TrackStats) intValues.next();
				sum.setListeners(sum.getListeners() + trackStats.getListeners());
				sum.setPlays(sum.getPlays() + trackStats.getPlays());
				sum.setSkips(sum.getSkips() + trackStats.getSkips());
				sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
				sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
			}
			context.write(trackId, sum);
		}
	}
	
	
	/**
	 * Mapper that takes the number of listeners for a track and converts this
	 * to a TrackStats object which is output against each track id.
	 */
	public static class MergeListenersMapper extends Mapper<IntWritable, IntWritable, IntWritable, TrackStats>{

		@Override
		protected void map(IntWritable trackId, IntWritable uniqueListenerCount, Context context) throws IOException,
				InterruptedException {
			TrackStats trackStats = new TrackStats();
			trackStats.setListeners(uniqueListenerCount.get());
			context.write(trackId, trackStats);
		}
	}
	
	/**
	 * Create a JobConf for a Job that will calculate the number of unique listeners per track.
	 * @param inputDir  The path to the folder containing the raw listening data  files.
	 * @return The unique listeners JobConf.
	 * @throws IOException 
	 */
	private Job getUniqueListenersJobConf(Path inputDir) throws IOException {
		log.info("Creating configuration for unique listeners Job");
		// output results to a temporary intermediate folder, this will get
		// deleted by start() method
		Path uniqueListenersOutput = new Path("/output/uniqueListeners");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		Job job = new Job(conf, " unique listeners Job");
		job.setJarByClass(TrackStatisticsProgram.class);
		job.setOutputKeyClass(IntWritable.class); // track id
		job.setOutputValueClass(IntWritable.class); // number of unique listeners
		job.setInputFormatClass(TextInputFormat.class); // raw listening data
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(UniqueListenersMapper.class);
		job.setCombinerClass(UniqueListenersCombiner.class);
		job.setReducerClass(UniqueListenersReducer.class);
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, uniqueListenersOutput);
		return job;
	}
	
	/**
	 * Creates a JobConf for a Job that will sum up the TrackStatistics per track.
	 * @param inputDir The path to the folder containing the raw input data files.
	 * @return The sum JobConf.
	 * @throws IOException 
	 */
	private Job getSumJobConf(Path inputDir) throws IOException {
		log.info("Creating configuration for sum job");
		// output results to a temporary intermediate folder, this will get
		// deleted by start() method
		Path playsOutput = new Path("/output/sum");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		Job job = new Job(conf, "sum");
		job.setJarByClass(TrackStatisticsProgram.class);
		job.setOutputKeyClass(IntWritable.class); // track id
		job.setOutputValueClass(TrackStats.class); // statistics for a track
		job.setInputFormatClass(TextInputFormat.class); // raw listening data
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(SumMapper.class);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);

		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, playsOutput);
		return job;
	}
	
	/**
	 * Creates a JobConf for a Job that will merge the unique listeners and
	 * track statistics.
	 * 
	 * @param outputPath The path for the results to be output to.
	 * @param sumInputDir The path containing the data from the sum Job.
	 * @param listenersInputDir The path containing the data from the unique listeners job.
	 * @return The merge JobConf.
	 * @throws IOException 
	 */
	private Job getMergeConf(Path outputPath, Path sumInputDir, Path listenersInputDir) throws IOException {
		log.info("Creating configuration for merge job");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		Job job = new Job(conf, "merge");
		job.setJarByClass(TrackStatisticsProgram.class);
		job.setOutputKeyClass(IntWritable.class); // track id
		job.setOutputValueClass(TrackStats.class); // overall track statistics
		job.setCombinerClass(SumReducer.class); // safe to re-use reducer as a
													// combiner here
		job.setReducerClass(SumReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		MultipleInputs.addInputPath(job, sumInputDir, SequenceFileInputFormat.class, Mapper.class);
		MultipleInputs.addInputPath(job, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);
		return job;
	}
	
	
	/**
	 * Start the program.
	 * 
	 * @param inputDir The path to the folder containing the raw listening data files.
	 * @param outputPath The path for the results to be output to.
	 * @throws IOException  If an error occurs retrieving data from the file system or an  error occurs running the job.
	 */
	public void start(Path inputDir, Path outputDir) throws IOException {
		FileSystem fs = FileSystem.get(this.conf);

		Job listenersJob = getUniqueListenersJobConf(inputDir);
		ControlledJob conListenersJob = new ControlledJob(listenersJob.getConfiguration());
		
		Path listenersOutputDir = FileOutputFormat.getOutputPath(listenersJob);
		// delete any output that might exist from a previous run of this job
		if (fs.exists(FileOutputFormat.getOutputPath(listenersJob))) {
			fs.delete(FileOutputFormat.getOutputPath(listenersJob), true);
		}

		Job sumJob = getSumJobConf(inputDir);
		ControlledJob conSumJob = new ControlledJob(sumJob.getConfiguration());
		
		Path sumOutputDir = FileOutputFormat.getOutputPath(sumJob);
		// delete any output that might exist from a previous run of this job
		if (fs.exists(FileOutputFormat.getOutputPath(sumJob))) {
			fs.delete(FileOutputFormat.getOutputPath(sumJob), true);
		}

		// the merge job depends on the other two jobs
		ArrayList<ControlledJob> mergeDependencies = new ArrayList<ControlledJob>();
		mergeDependencies.add(conListenersJob);
		mergeDependencies.add(conSumJob);
		
		Job mergeJob = getMergeConf(outputDir, sumOutputDir, listenersOutputDir);
		ControlledJob conMergeJob = new ControlledJob(mergeJob, mergeDependencies);
		// delete any output that might exist from a previous run of this job
		if (fs.exists(FileOutputFormat.getOutputPath(mergeJob))) {
			fs.delete(FileOutputFormat.getOutputPath(mergeJob), true);
		}

		// store the output paths of the intermediate jobs so this can be cleaned up after a successful run
		List<Path> deletePaths = new ArrayList<Path>();
		deletePaths.add(FileOutputFormat.getOutputPath(listenersJob));
		deletePaths.add(FileOutputFormat.getOutputPath(sumJob));

		JobControl control = new JobControl("TrackStatisticsProgram");
		control.addJob(conListenersJob);
		control.addJob(conSumJob);
		control.addJob(conMergeJob);
		

		// execute the jobs
		try {
			Thread jobControlThread = new Thread(control, "jobcontrol");
			jobControlThread.start();
			while (!control.allFinished()) {
				Thread.sleep(1000);
			}
			if (control.getFailedJobList().size() > 0) {
				throw new IOException("One or more jobs failed");
			}
		} catch (InterruptedException e) {
			throw new IOException("Interrupted while waiting for job control to finish", e);
		}

		// remove intermediate output paths  不删除前面两个作业的输出结结果
		/*for (Path deletePath : deletePaths) {
			fs.delete(deletePath, true);
		}*/
	}
	
	 /**
	   * Set the Configuration used by this Program.
	   * @param conf The new Configuration to use by this program.
	   */
	  public void setConf(Configuration conf) {
	    this.conf = conf; // this will usually only be set by unit test.
	  }

	  /**
	   * Gets the Configuration used by this program.
	   * @return This program's Configuration.
	   */
	  public Configuration getConf() {
	    return conf;
	  }

	  /**
	   * Main method used to run the TrackStatisticsProgram from the command line. This takes two parameters - first the
	   * path to the folder containing the raw input data; and second the path for the data to be output to.
	   * 
	   * @param args Command line arguments.
	   * @throws IOException If an error occurs running the program.
	   */
	  public static void main(String[] args) throws Exception {
	    Path inputPath = new Path("/input/fmlast.txt");
	    Path outputDir = new Path("/output/20130614");
	    log.info("Running on input directories: " + inputPath);
	    TrackStatisticsProgram listeners = new TrackStatisticsProgram();
	    listeners.start(inputPath, outputDir);
	    //Job job1 = listeners.getUniqueListenersJobConf(inputPath);
	    //System.exit(job1.waitForCompletion(true) ? 0 : 1);
	  }
}