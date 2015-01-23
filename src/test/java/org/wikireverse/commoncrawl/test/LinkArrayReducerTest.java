package org.wikireverse.commoncrawl.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.mockito.Matchers;

import org.wikireverse.commoncrawl.LinkArrayWritable;
import org.wikireverse.commoncrawl.LinkArrayReducer;
import org.wikireverse.commoncrawl.LinkWritable;

public class LinkArrayReducerTest {

	// Constants for reducer specific counters.
	private static final String COUNTER_GROUP = "LinkArray Reducer Counters";
	private static final String URLS_REDUCED = "URLs Reduced";
	private static final String RESULTS_COMBINED = "Results Combined";
	
	@Test
	public void combineSingleArrayWithSingleValue() throws IOException, InterruptedException  {		
		Text key = new Text(TestHelper.WIKI_KEY_1);
		
		LinkWritable link = new LinkWritable(TestHelper.TITLE_TAG, TestHelper.DATE, TestHelper.EXAMPLE_URL, TestHelper.DESCRIPTION);	
		
		LinkWritable[] linkValues = new LinkWritable[1];
		linkValues[0] = link;
		
		LinkArrayWritable value = new LinkArrayWritable();
		value.set(linkValues);
		
		ArrayList<LinkArrayWritable> values = new ArrayList<LinkArrayWritable>();
		values.add(value);
		
		performReduce(key, values, 1);
	}
	
	@Test
	public void convertTwoArraysWithFiveValues() throws IOException, InterruptedException  {
		String title = TestHelper.TITLE_TAG;
		String date = TestHelper.DATE;
		String description = TestHelper.DESCRIPTION;
		
		Text key = new Text(TestHelper.WIKI_KEY_1);		
		
		LinkWritable link1 = new LinkWritable(title, date, TestHelper.RESULT_1, description);
		LinkWritable link2 = new LinkWritable(title, date, TestHelper.RESULT_2, description);		
		LinkWritable link3 = new LinkWritable(title, date, TestHelper.RESULT_3, description);
		
		LinkWritable[] linkValues1 = new LinkWritable[3];
		linkValues1[0] = link1;
		linkValues1[1] = link2;
		linkValues1[2] = link3;
		
		LinkArrayWritable linkArray1 = new LinkArrayWritable();
		linkArray1.set(linkValues1);
				
		LinkWritable link4 = new LinkWritable(title, date, TestHelper.RESULT_4, description);
		LinkWritable link5 = new LinkWritable(title, date, TestHelper.RESULT_5, description);	
		
		LinkWritable[] linkValues2 = new LinkWritable[2];
		linkValues2[0] = link4;
		linkValues2[1] = link5;				
		
		LinkArrayWritable linkArray2 = new LinkArrayWritable();
		linkArray2.set(linkValues2);
		
		ArrayList<LinkArrayWritable> values = new ArrayList<LinkArrayWritable>();
		values.add(linkArray1);
		values.add(linkArray2);
			
		performReduce(key, values, 5);
	}
		
	private void performReduce(Text key, ArrayList<LinkArrayWritable> values, int urlCount) throws IOException, InterruptedException {		
		// Mock Hadoop outputs.
		OutputCollector<Text, LinkArrayWritable> output = mock(OutputCollector.class);
		Reporter reporter = mock(Reporter.class);

		LinkArrayReducer reducer = new LinkArrayReducer();
		reducer.reduce(key, values.iterator(), output, reporter);
		
		verify(output).collect(Matchers.eq(key), Matchers.any(LinkArrayWritable.class));
		verify(reporter).incrCounter(COUNTER_GROUP, URLS_REDUCED, 1);
		verify(reporter).incrCounter(COUNTER_GROUP, RESULTS_COMBINED, urlCount);
	}
}