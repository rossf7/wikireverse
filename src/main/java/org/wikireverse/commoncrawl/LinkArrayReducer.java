package org.wikireverse.commoncrawl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/*
 * A Hadoop custom reducer for the LinkArrayWritable class. Combines arrays of LinkWritable objects.
 * 
 * @author Ross Fairbanks
 */
public class LinkArrayReducer extends MapReduceBase implements Reducer<Text, LinkArrayWritable, Text, LinkArrayWritable> {
	private static final Logger LOG = Logger.getLogger(LinkArrayReducer.class);

	// Constants for reducer specific counters.
	private static final String COUNTER_GROUP = "LinkArray Reducer Counters";
	private static final String URLS_REDUCED = "URLs Reduced";
	private static final String RESULTS_COMBINED = "Results Combined";
	private static final String REDUCE_EXCEPTION = "Reduce - Other Exception";

	/*
	 * Combines the inputs it receives into a single LinkArrayWritable. 
	 */
	public void reduce(Text key, Iterator<LinkArrayWritable> values, OutputCollector<Text, LinkArrayWritable> output, Reporter reporter) throws IOException {
		try {
			LinkArrayWritable value = new LinkArrayWritable();
			Writable[] allValues = new Writable[0];
			Writable[] combinedValues;
			Writable[] nextValues;
			
			while (values.hasNext()) {
				nextValues = values.next().get();
				combinedValues = new Writable[allValues.length + nextValues.length];
				
				System.arraycopy(allValues, 0, combinedValues, 0, allValues.length);
				System.arraycopy(nextValues, 0, combinedValues, allValues.length, nextValues.length);
				
				allValues = combinedValues;
			}
			
			value.set(allValues);
			output.collect(key, value);
			
			reporter.incrCounter(COUNTER_GROUP, URLS_REDUCED, 1);
			reporter.incrCounter(COUNTER_GROUP, RESULTS_COMBINED, allValues.length);		

		} catch (Exception e) {
			reporter.incrCounter(COUNTER_GROUP, REDUCE_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(e));
		}
	}	
}