package org.wikireverse.commoncrawl.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.wikireverse.commoncrawl.LinkArrayWritable;
import org.wikireverse.commoncrawl.LinkWritable;

public class LinkArrayWritableTest {

	private static final String ARTICLE = "ZX_Spectrum";
	private static final String TITLE = "Home Page";
	private static final String DATE = "2013-12-22T11:53:38Z";
	private static final String URL = "https://www.example.com";
	
	@Test
	public void getSingleArticleName() throws IOException, InterruptedException  {
		LinkWritable[] linkValues = new LinkWritable[1];
		linkValues[0] = createLinkWritable(ARTICLE);
		
		LinkArrayWritable linkArray = new LinkArrayWritable();
		linkArray.set(linkValues);
		
		assertEquals(ARTICLE, linkArray.getMostUsedArticleCasing());
	}

	@Test
	public void getCorrectArticleName() throws IOException, InterruptedException  {
		LinkWritable[] linkValues = new LinkWritable[3];
		linkValues[0] = createLinkWritable(ARTICLE);
		linkValues[1] = createLinkWritable(ARTICLE);
		linkValues[2] = createLinkWritable(ARTICLE.toLowerCase());
		
		LinkArrayWritable linkArray = new LinkArrayWritable();
		linkArray.set(linkValues);
		
		assertEquals(ARTICLE, linkArray.getMostUsedArticleCasing());
	}

	private LinkWritable createLinkWritable(String articleName) {
		String title = TITLE;
		String date = DATE;
		String url = URL;
		
		return new LinkWritable(articleName, title, date, url);
	}
}