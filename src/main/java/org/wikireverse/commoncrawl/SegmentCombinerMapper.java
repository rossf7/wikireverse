package org.wikireverse.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/*
 * A Hadoop custom mapper that outputs all parsed results from multiple Common Crawl
 * segments so they are combined into a single set of results.
 * 
 * @author Ross Fairbanks
 */
public class SegmentCombinerMapper extends MapReduceBase 
	implements Mapper<Text, LinkArrayWritable, Text, LinkArrayWritable> {

	private static final Logger LOG = Logger.getLogger(SegmentCombinerMapper.class);
	
	// Constants for mapper specific counters.
	private static final String COUNTER_GROUP = "Combiner Mapper Counters";	
	private static final String RECORDS_FETCHED = "Records Fetched";
	private static final String RESULTS_COUNTED = "Results Counted";
	private static final String MAP_EXCEPTION = "Map - Other Exception";
	
	public void map(Text key, LinkArrayWritable value, OutputCollector<Text, LinkArrayWritable> output, Reporter reporter)
	        throws IOException {

		try {
			output.collect(key, value);

			reporter.incrCounter(COUNTER_GROUP, RECORDS_FETCHED, 1);
			reporter.incrCounter(COUNTER_GROUP, RESULTS_COUNTED, value.get().length);
			
		} catch (Exception e) {
			reporter.incrCounter(COUNTER_GROUP, MAP_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(e));
		}
	}
}