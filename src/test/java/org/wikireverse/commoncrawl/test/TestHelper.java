package org.wikireverse.commoncrawl.test;

import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.mockito.Mockito;
import org.apache.hadoop.io.Text;

import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

public class TestHelper {
	public static final String WIKIPEDIA_DOMAIN = ".wikipedia.org";
	public static final String LINK_TYPE = "A@/href";
	
	// Test page data
	public static final String TITLE_TAG = "Home Page";
	public static final String DATE = "2013-12-22T11:53:38Z";
	public static final String DESCRIPTION = "Description for home page";
	
	public static final String TITLE_TAG_CONTAINS_WHITESPACE = "Title contains whitespace";
	
	public static final String SITE_EN = "en";	
	public static final String SITE_ES = "es";	
	public static final String SITE_PT = "pt";	
	
	public static final String EXAMPLE_URL = "http://www.example.com";
	
	public static final String WIKI_URL_1 = "http://en.wikipedia.org/wiki/Danger_Zone_(song)";	
	public static final String WIKI_URL_2 = "http://en.wikipedia.org/wiki/Mad_max";	
	public static final String WIKI_URL_3 = "http://en.wikipedia.org/wiki/ZX_Spectrum";	
	public static final String WIKI_URL_4 = "http://es.wikipedia.org/wiki/Tibidabo";	
	public static final String WIKI_URL_5 = "http://pt.wikipedia.org/wiki/Bateria_de_l%C3%ADtio";	
	public static final String WIKI_URL_6 = "http://en.wikipedia.org/wiki/Talk:Danger_Zone_(song)";	
	public static final String WIKI_URL_7 = "http://en.wikipedia.org/wiki/File:ZXSpectrum48k.jpg";	
	
	public static final String WIKI_ARTICLE_1 = "Danger_Zone_(song)";
	public static final String WIKI_ARTICLE_2 = "Mad_max";
	public static final String WIKI_ARTICLE_3 = "ZX_Spectrum";
	public static final String WIKI_ARTICLE_4 = "Tibidabo";
	public static final String WIKI_ARTICLE_5 = "Bateria_de_lítio";
	public static final String WIKI_ARTICLE_6 = "Captcha";

	public static final String WIKI_KEY_1 = SITE_EN + "\t" + WIKI_ARTICLE_1.toLowerCase();
	public static final String WIKI_KEY_2 = SITE_EN + "\t" + WIKI_ARTICLE_2.toLowerCase();
	public static final String WIKI_KEY_3 = SITE_EN + "\t" + WIKI_ARTICLE_3.toLowerCase();
	public static final String WIKI_KEY_4 = SITE_ES + "\t" + WIKI_ARTICLE_4.toLowerCase();
	public static final String WIKI_KEY_5 = SITE_PT + "\t" + WIKI_ARTICLE_5.toLowerCase();
	public static final String WIKI_KEY_6 = SITE_EN + "\t" + WIKI_ARTICLE_6.toLowerCase();

	public static String RESULT_1 = "https://example.com/page1.html?page=home&test=test";
	public static String RESULT_2 = "https://example.com/page2.html";
	public static String RESULT_3 = "http://example.com/page3.html";
	public static String RESULT_4 = "https://example.org/page1.html?page=home&test=test";
	public static String RESULT_5 = "https://example.org/page2.html";
	
	// Example Metadata files for parsing.
	public static final String NO_WIKI_ARTICLES = "no_wiki_articles.json";
	public static final String NO_WIKI_TEXT = "no_wiki_text.json";
	public static final String THREE_WIKI_ARTICLES = "three_wiki_articles.json";
	public static final String TWO_WIKI_ARTICLES = "two_wiki_articles.json";
	public static final String ONE_WIKI_ARTICLE = "one_wiki_article.json";
	public static final String EDIT_LINK = "edit_link.json";
	public static final String ONE_WIKI_ARTICLE_TWO_WIKI_PAGES = "one_wiki_article_two_wiki_pages.json";
	public static final String TITLE_CONTAINS_WHITESPACE = "title_contains_whitespace.json";
	public static final String FAILURE_RESPONSE = "failure_response.json";
	public static final String LONG_TITLE_TAG = "long_title_tag.json";
	public static final String ONE_VALID_ONE_INVALID_WIKI_LINKS = "one_valid_one_invalid_wiki_links.json";
	public static final String ONE_VALID_THREE_INVALID_WIKI_LINKS = "one_valid_three_invalid_wiki_links.json";
	public static final String DIFFERENT_CASE_WIKI_SUBDOMAINS = "different_case_wiki_subdomains.json";
	public static final String WIKI_TEXT_NO_WIKI_ARTICLES = "wiki_text_no_wiki_articles.json";

	public static WritableWarcRecord getWritableWarcRecord(String url, String testFileName) throws IOException {
		Text metadata = getMetadata(testFileName);

		WritableWarcRecord warcWritable = mock(WritableWarcRecord.class);
		WarcRecord warcRecord = mock(WarcRecord.class);
		
		Mockito.when(warcWritable.getRecord()).thenReturn(warcRecord);		
		Mockito.when(warcRecord.getHeaderMetadataItem("WARC-Target-URI")).thenReturn(url);
		Mockito.when(warcRecord.getContent()).thenReturn(metadata.getBytes());
		
		return warcWritable;
	}
	
	public static Text getMetadata(String testFileName) throws IOException {
		StringBuffer metadata = new StringBuffer();
		
		InputStream in = TestHelper.class.getResourceAsStream(testFileName);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		char[] buf = new char[1024];
		int numRead = 0;

		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			metadata.append(readData);
			buf = new char[1024];
		}
		reader.close();
		
		return new Text(metadata.toString());
	}
}