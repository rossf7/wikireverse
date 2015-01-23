package org.wikireverse.commoncrawl.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.mockito.Matchers;
import org.wikireverse.commoncrawl.LinkArrayWritable;
import org.wikireverse.commoncrawl.WikiMetadata;
import org.wikireverse.commoncrawl.WikiReverseMapper;
import org.wikireverse.commoncrawl.test.TestHelper;

import edu.cmu.lemurproject.WritableWarcRecord;

public class WikiReverseMapperTest {

	private static final String COUNTER_GROUP = "Wiki Metadata Counters";
	private static final String SKIP_WIKIPEDIA_PAGE = "Skipped - Wikipedia Page";

	@Test
	public void parsePageWithNoWikiText() throws IOException, InterruptedException {
		OutputCollector<Text, LinkArrayWritable> output = parse(TestHelper.EXAMPLE_URL,
				TestHelper.NO_WIKI_TEXT);

		// Verify no results were found.
		verifyZeroInteractions(output);
	}
	
	@Test
	public void parsePageWithNoWikiArticles() throws IOException, InterruptedException {
		OutputCollector<Text, LinkArrayWritable> output = parse(TestHelper.EXAMPLE_URL,
				TestHelper.NO_WIKI_ARTICLES);

		// Verify no results were found.
		verifyZeroInteractions(output);
	}
	
	@Test
	public void parsePageWithThreeWikiLinks() throws IOException, InterruptedException {
		OutputCollector<Text, LinkArrayWritable> output = parse(TestHelper.EXAMPLE_URL,
				TestHelper.THREE_WIKI_ARTICLES);
				
		// Verify 3 results were found.
		verify(output, times(3)).collect(Matchers.any(Text.class), Matchers.any(LinkArrayWritable.class));
	}

	@Test
	public void parsePageWithOneWikiLink() throws IOException, InterruptedException {
		OutputCollector<Text, LinkArrayWritable> output = parse(TestHelper.EXAMPLE_URL,
				TestHelper.ONE_WIKI_ARTICLE);
				
		// Verify 1 result was found.
		verify(output, times(1)).collect(Matchers.any(Text.class), Matchers.any(LinkArrayWritable.class));
	}
	
	@Test
	public void parseWikipediaPage() throws IOException, InterruptedException {
		// Set up inputs.
		LongWritable key = new LongWritable(12345);
		WritableWarcRecord value = TestHelper.getWritableWarcRecord(TestHelper.WIKI_URL_1, TestHelper.NO_WIKI_ARTICLES);
		
		// Mock Hadoop outputs.
		OutputCollector<Text, LinkArrayWritable> output = mock(OutputCollector.class);
		Reporter reporter = mock(Reporter.class);
		
		// Perform map.
		WikiReverseMapper mapper = new WikiReverseMapper();
		WikiMetadata wikiMetadata = new WikiMetadata();

		mapper.map(key, value, output, reporter, wikiMetadata);
		
		// Verify no results were found.
		verifyZeroInteractions(output);
		verify(reporter).incrCounter(COUNTER_GROUP, SKIP_WIKIPEDIA_PAGE, 1);
	}
	
	
	private OutputCollector<Text, LinkArrayWritable> parse(String testUrl, String testFilename) throws IOException, InterruptedException {
		// Set up inputs.
		LongWritable key = new LongWritable(12345);
		WritableWarcRecord value = TestHelper.getWritableWarcRecord(testUrl, testFilename);
		
		// Mock Hadoop outputs.
		OutputCollector<Text, LinkArrayWritable> output = mock(OutputCollector.class);
		Reporter reporter = mock(Reporter.class);
		
		// Perform map.
		WikiReverseMapper mapper = new WikiReverseMapper();
		WikiMetadata wikiMetadata = new WikiMetadata();
		
		mapper.map(key, value, output, reporter, wikiMetadata);
				
		return output;	
	}
}
