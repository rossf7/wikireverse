package org.wikireverse.commoncrawl.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import org.wikireverse.commoncrawl.SegmentCombinerMapper;
import org.wikireverse.commoncrawl.LinkArrayWritable;
import org.wikireverse.commoncrawl.LinkWritable;

public class SegmentCombinerMapperTest {

	// Constants for reducer specific counters.
	private static final String COUNTER_GROUP = "Combiner Mapper Counters";	
	private static final String RECORDS_FETCHED = "Records Fetched";
	private static final String RESULTS_COUNTED = "Results Counted";
		
	@Test
	public void combineOneUrl() throws IOException, InterruptedException  {
		LinkWritable[] linkValues = new LinkWritable[1];
		LinkWritable link = new LinkWritable(TestHelper.TITLE_TAG, TestHelper.DATE, TestHelper.RESULT_1, TestHelper.DESCRIPTION);
		linkValues[0] = link;
		
		LinkArrayWritable linkArray = new LinkArrayWritable();
		linkArray.set(linkValues);
		
		Text key = new Text(TestHelper.WIKI_KEY_1);
		
		performOutputMap(key, linkArray, 1);
	}
	
	@Test
	public void combineThreeUrls() throws IOException, InterruptedException  {
		String title = TestHelper.TITLE_TAG;
		String date = TestHelper.DATE;
		String description = TestHelper.DESCRIPTION;

		LinkWritable link1 = new LinkWritable(title, date, TestHelper.RESULT_1, description);
		LinkWritable link2 = new LinkWritable(title, date, TestHelper.RESULT_2, description);		
		LinkWritable link3 = new LinkWritable(title, date, TestHelper.RESULT_3, description);
		
		LinkWritable[] linkValues = new LinkWritable[3];
		linkValues[0] = link1;
		linkValues[1] = link2;
		linkValues[2] = link3;
		
		LinkArrayWritable linkArray = new LinkArrayWritable();
		linkArray.set(linkValues);
				
		Text key = new Text(TestHelper.WIKI_KEY_1);
		
		performOutputMap(key, linkArray, 3);
	}
	
	private void performOutputMap(Text key, LinkArrayWritable value, int valueCount) throws IOException, InterruptedException {		
		// Mock Hadoop outputs.
		OutputCollector<Text, LinkArrayWritable> output = mock(OutputCollector.class);
		Reporter reporter = mock(Reporter.class);

		SegmentCombinerMapper mapper = new SegmentCombinerMapper();
		mapper.map(key, value, output, reporter);
		
		verify(output).collect(key, value);
		verify(reporter).incrCounter(COUNTER_GROUP, RECORDS_FETCHED, 1);
		verify(reporter).incrCounter(COUNTER_GROUP, RESULTS_COUNTED, valueCount);
		
	}
}