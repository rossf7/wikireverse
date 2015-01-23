package org.wikireverse.commoncrawl;

import java.util.HashSet;

/*
 * Page data that has been extracted from a metadata entry by the MetadataParser.
 * 
 * @author Ross Fairbanks
 */
public class Page {
	private String url;
	private String title;
	private String warcDate;
	private HashSet<String> linkUrls;

	public Page(String url) {
		this.url = url;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getWarcDate() {
		return warcDate;
	}

	public void setWarcDate(String warcDate) {
		this.warcDate = warcDate;
	}

	public HashSet<String> getLinkUrls() {
		return linkUrls;
	}

	public void setLinkUrls(HashSet<String> linkUrls) {
		this.linkUrls = linkUrls;
	}
}
