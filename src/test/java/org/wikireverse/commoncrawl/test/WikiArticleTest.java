package org.wikireverse.commoncrawl.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.wikireverse.commoncrawl.WikiArticle;

public class WikiArticleTest {

	@Test
	public void constructorTest()   {		
		String subdomain = TestHelper.SITE_EN;
		String articleName = TestHelper.WIKI_ARTICLE_1;
		
		WikiArticle article = new WikiArticle(subdomain, articleName);
		
		assertEquals(subdomain, article.getSubdomain());
		assertEquals(articleName, article.getArticleName());
	}

	@Test
	public void keyOutputTest()   {		
		String subdomain = TestHelper.SITE_EN;
		String articleName = TestHelper.WIKI_ARTICLE_1;
		
		WikiArticle article = new WikiArticle(subdomain, articleName);
		
		assertEquals(TestHelper.WIKI_KEY_1, article.getKey());
	}

	@Test
	public void keyOutputTrailingSlashTest()   {		
		String subdomain = TestHelper.SITE_EN;
		String articleName = TestHelper.WIKI_ARTICLE_1;
		
		WikiArticle article = new WikiArticle(subdomain, articleName.concat("\\\\"));
		assertEquals(TestHelper.WIKI_KEY_1, article.getKey());

		article = new WikiArticle(subdomain, articleName.concat("\\"));
		assertEquals(TestHelper.WIKI_KEY_1, article.getKey());
	}
}
