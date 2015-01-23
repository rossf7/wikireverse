package org.wikireverse.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/*
 * A Hadoop custom mapper that outputs results in text format.
 * 
 * @author Ross Fairbanks
 */
public class OutputToTextMapper extends MapReduceBase 
	implements Mapper<Text, LinkArrayWritable, Text, Text> {

	private static final Logger LOG = Logger.getLogger(OutputToTextMapper.class);
	
	// Constants for mapper specific counters.
	private static final String COUNTER_GROUP = "OutputToText Mapper Counters";	
	private static final String RECORDS_FETCHED = "Records Fetched";
	private static final String RESULTS_OUTPUT = "Results Output";
	private static final String MAP_EXCEPTION = "Map - Other Exception";

	/*
	 * Map outputs each LinkWritable value in text format. If an article name appears in multiple cases
	 * the most widely used casing is returned. This is so the article name is displayed correctly.
	 */
	public void map(Text lowerCaseKey, LinkArrayWritable value, OutputCollector<Text, Text> output, Reporter reporter)
	        throws IOException {
		
		try {
			int recordCount = 0;
			Text outputValue = new Text();

			String correctCase = value.getMostUsedArticleCasing();
			String correctKey = lowerCaseKey.toString().replace(correctCase.toLowerCase(), correctCase);
			
			if (correctKey.endsWith("//")) {
				correctKey = correctKey.replaceAll("////", "");
			}
		
			Text key = new Text(correctKey);
			
			for (Writable rawValue : value.get()) {
				LinkWritable link = (LinkWritable)rawValue;
				outputValue.set(link.toString());
	
				output.collect(key, outputValue);
				recordCount++;
			}
			
			reporter.incrCounter(COUNTER_GROUP, RECORDS_FETCHED, 1);
			reporter.incrCounter(COUNTER_GROUP, RESULTS_OUTPUT, recordCount);
			
		} catch(Exception e) {
			reporter.incrCounter(COUNTER_GROUP, MAP_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(e));
		}
	}
}