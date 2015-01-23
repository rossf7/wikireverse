package org.wikireverse.commoncrawl;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/*
 * A Hadoop job launched by the elasticrawl combine command.
 * 
 * Aggregates the results of multiple Common Crawl segments
 * into a single set of files.
 * 
 * @author Ross Fairbanks
 */
public class SegmentCombiner extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(SegmentCombiner.class);

	/*
	 * Entry point that runs the Hadoop job.
	 * 
	 * @param args array of input arguments
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = 0;
		exitCode = ToolRunner.run(new SegmentCombiner(), args);
		System.exit(exitCode);
	}

	/*
	 * Runs the Hadoop job which takes in 2 arguments.
	 * 
	 * Input Paths: Comma separated list of S3 locations
	 * 				containing parse job results.
	 * 				A wildcard is used to match all segments.
	 * Output Path: S3 location for storing the combined results.
	 * 
	 * @param args array of input arguments
	 * 
	 * @return result code for the job
	 */
	public int run(String[] args) throws Exception {
		// Get current configuration.
		Configuration conf = getConf();

		// Parse command line arguments.
		String inputPaths = args[0];
		String outputPath = args[1];

		JobConf job = new JobConf(conf);

		// Set input path.
		if (inputPaths.length() > 0) {
			List<String> segmentPaths = Lists.newArrayList(Splitter.on(",")
					.split(inputPaths));

			for (String segmentPath : segmentPaths) {
				LOG.info("Adding input path " + segmentPath);
				FileInputFormat.addInputPath(job, new Path(segmentPath));
			}
		} else {
			System.err.println("No input path found.");
			return 1;
		}

		// Set output path.
		if (outputPath.length() > 0) {
			LOG.info("Setting output path to " + outputPath);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
			// Compress output to boost performance.
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
		} else {
			System.err.println("No output path found.");
			return 1;
		}

		// Load other classes from same jar as this class.
		job.setJarByClass(SegmentCombiner.class);

		// Input is Hadoop sequence file format.
		job.setInputFormat(SequenceFileInputFormat.class);

		// Output is Hadoop sequence file format.
		job.setOutputFormat(SequenceFileOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LinkArrayWritable.class);

		// Use custom mapper class.
		job.setMapperClass(SegmentCombinerMapper.class);

		// Use custom reducer class.
		job.setReducerClass(LinkArrayReducer.class);

		if (JobClient.runJob(job).isSuccessful())
			return 0;
		else
			return 1;
	}
}