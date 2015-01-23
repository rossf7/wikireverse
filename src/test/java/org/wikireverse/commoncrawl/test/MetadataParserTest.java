package org.wikireverse.commoncrawl.test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.wikireverse.commoncrawl.MetadataParser;
import org.wikireverse.commoncrawl.Page;

public class MetadataParserTest {
	private static final String WIKIPEDIA_DOMAIN = ".wikipedia.org";
	private static final String LINK_TYPE = "A@/href";
	
	@Test
	public void parsePageWithNoWikiText() throws IOException, URISyntaxException {
		Page page = parse(TestHelper.RESULT_1, TestHelper.NO_WIKI_TEXT);
		
		assertEquals(0, page.getLinkUrls().size());
	}

	@Test
	public void parsePageWithThreeWikiArticles() throws IOException, URISyntaxException {
		Page page = parse(TestHelper.RESULT_1, TestHelper.THREE_WIKI_ARTICLES);
		
		assertEquals(3, page.getLinkUrls().size());
	}

	private Page parse(String url, String testFileName) throws IOException, URISyntaxException {
		Text metadata = TestHelper.getMetadata(testFileName);
		Reporter reporter = mock(Reporter.class);
		Page page = new Page(url);
		
		page = MetadataParser.parse(page, metadata, LINK_TYPE, WIKIPEDIA_DOMAIN);
		
		return page;
	}
}