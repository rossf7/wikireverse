package org.wikireverse.commoncrawl.test;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.wikireverse.commoncrawl.LinkWritable;
import org.wikireverse.commoncrawl.MetadataParser;
import org.wikireverse.commoncrawl.Page;
import org.wikireverse.commoncrawl.WikiArticle;
import org.wikireverse.commoncrawl.WikiMetadata;

import com.fasterxml.jackson.core.JsonParseException;

public class WikiMetadataTest {

	public void parsePageWithNoWikiLinks() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.NO_WIKI_ARTICLES);
		
		assertEquals(0, links.size());
	}
	
	@Test
	public void parseThreeWikiLinks() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.THREE_WIKI_ARTICLES);
		
		assertEquals(3, links.size());
		
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_1));		
		LinkWritable link1 = links.get(TestHelper.WIKI_KEY_1);
		
		assertEquals(TestHelper.WIKI_ARTICLE_1, link1.getArticle().toString());
		assertEquals(TestHelper.TITLE_TAG, link1.getTitle().toString());
		assertEquals(TestHelper.DATE, link1.getDate().toString());
		assertEquals(TestHelper.RESULT_1, link1.getUrl().toString());
		
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_2));
		LinkWritable link2 = links.get(TestHelper.WIKI_KEY_2);

		assertEquals(TestHelper.WIKI_ARTICLE_2, link2.getArticle().toString());
		assertEquals(TestHelper.TITLE_TAG, link2.getTitle().toString());
		assertEquals(TestHelper.DATE, link2.getDate().toString());
		assertEquals(TestHelper.RESULT_1, link2.getUrl().toString());
		
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_3));
		LinkWritable link3 = links.get(TestHelper.WIKI_KEY_3);

		assertEquals(TestHelper.WIKI_ARTICLE_3, link3.getArticle().toString());
		assertEquals(TestHelper.TITLE_TAG, link3.getTitle().toString());
		assertEquals(TestHelper.DATE, link3.getDate().toString());
		assertEquals(TestHelper.RESULT_1, link3.getUrl().toString());
	}

	@Test
	public void parseOneWikiLink() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_4, TestHelper.ONE_WIKI_ARTICLE);
		String result = TestHelper.WIKI_KEY_6;
		
		assertEquals(1, links.size());
		assertTrue(links.containsKey(result));

		LinkWritable link = links.get(result);

		assertEquals(TestHelper.WIKI_ARTICLE_6, link.getArticle().toString());
		assertEquals(TestHelper.TITLE_TAG, link.getTitle().toString());
		assertEquals(TestHelper.DATE, link.getDate().toString());
		assertEquals(TestHelper.RESULT_4, link.getUrl().toString());
	}

	@Test
	public void parseDifferentCaseWikiLinks() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_4, TestHelper.DIFFERENT_CASE_WIKI_SUBDOMAINS);
		assertEquals(2, links.size());
	}
	
	
	@Test
	public void parseOneValidOneInvalidWikiLinks() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.ONE_VALID_ONE_INVALID_WIKI_LINKS);
		
		assertEquals(1, links.size());
		
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_1));		
		LinkWritable link = links.get(TestHelper.WIKI_KEY_1);
		
		assertEquals(TestHelper.WIKI_ARTICLE_1, link.getArticle().toString());
		assertEquals(TestHelper.TITLE_TAG, link.getTitle().toString());
		assertEquals(TestHelper.DATE, link.getDate().toString());
		assertEquals(TestHelper.RESULT_1, link.getUrl().toString());
	}

	@Test
	public void parsePageWithEditLink() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.EDIT_LINK);
		
		assertEquals(0, links.size());
	}

	@Test
	public void parseOneWikiArticlesSkipTwoWikiPages() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.ONE_WIKI_ARTICLE_TWO_WIKI_PAGES);
		
		assertEquals(1, links.size());
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_5));		
	}
	
	@Test
	public void parseTitleContainingWhitespace() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.TITLE_CONTAINS_WHITESPACE);
		
		assertEquals(1, links.size());
		
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_1));		
		LinkWritable link = links.get(TestHelper.WIKI_KEY_1);
		
		assertEquals(TestHelper.TITLE_TAG_CONTAINS_WHITESPACE, link.getTitle().toString());
	}

	@Test
	public void parseLongTitleEnsureIsTruncated() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.LONG_TITLE_TAG);
		
		assertEquals(1, links.size());
		
		assertTrue(links.containsKey(TestHelper.WIKI_KEY_1));		
		LinkWritable link1 = links.get(TestHelper.WIKI_KEY_1);
		
		assertEquals(70, link1.getTitle().toString().length());
	}

	@Test
	public void parseNoWikiArticles() throws IOException, URISyntaxException {
		Hashtable<String, LinkWritable> links = createResults(TestHelper.RESULT_1, TestHelper.WIKI_TEXT_NO_WIKI_ARTICLES);
		
		assertEquals(0, links.size());
	}

	private Hashtable<String, LinkWritable> createResults(String url, String testFileName) throws IOException, URISyntaxException {
		Text metadata = TestHelper.getMetadata(testFileName);
		Reporter reporter = mock(Reporter.class);
		
		Page page = MetadataParser.parse(new Page(url), metadata, TestHelper.LINK_TYPE, TestHelper.WIKIPEDIA_DOMAIN);
		WikiMetadata wikiMetadata = new WikiMetadata();
		
		return wikiMetadata.createResults(page, reporter);
	}
}