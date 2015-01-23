package org.wikireverse.commoncrawl.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import org.wikireverse.commoncrawl.LinkWritable;

public class LinkWritableTest {

	private static final String ARTICLE = "ZX_Spectrum";
	private static final String TITLE = "Home Page";
	private static final String DATE = "2013-12-22T11:53:38Z";
	private static final String HOST_NAME = "www.example.com";
	private static final String URL = "https://www.example.com";
	
	@Test
	public void emptyConstructorTest() throws IOException, InterruptedException  {
		LinkWritable link = new LinkWritable();
		
		assertEquals("", link.getArticle().toString());
		assertEquals("", link.getTitle().toString());
		assertEquals("", link.getDate().toString());
		assertEquals("", link.getUrl().toString());	
	}
	
	@Test
	public void writableTypesConstructorTest() throws IOException, InterruptedException  {		
		Text article = new Text(ARTICLE);
		Text title = new Text(TITLE);
		Text date = new Text(DATE);
		Text url = new Text(URL);
		
		LinkWritable link = new LinkWritable(article, title, date, url);
		
		assertEquals(article, link.getArticle());
		assertEquals(title, link.getTitle());
		assertEquals(date, link.getDate());
		assertEquals(url, link.getUrl());	
	}
	
	@Test
	public void javaTypesConstructorTest() throws IOException, InterruptedException  {		
		String article = ARTICLE;
		String title = TITLE;
		String date = DATE;
		String url = URL;
		
		LinkWritable link = new LinkWritable(article, title, date, url);
		
		assertEquals(article, link.getArticle().toString());
		assertEquals(title, link.getTitle().toString());
		assertEquals(date, link.getDate().toString());
		assertEquals(url, link.getUrl().toString());	
	}
	
	@Test
	public void textOutputTest() throws IOException, InterruptedException  {		
		StringBuilder output = new StringBuilder();
		String article = ARTICLE;
		String title = TITLE;
		String date = DATE;
		String hostName = HOST_NAME;
		String url = URL;
		
		output.append(date);
		output.append("\t");
		output.append(title);
		output.append("\t");
		output.append(hostName);	
		output.append("\t");
		output.append(url);	
		
		LinkWritable link = new LinkWritable(article, title, date, url);
		
		assertEquals(output.toString(), link.toString());	
	}
}