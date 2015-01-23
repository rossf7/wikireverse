package org.wikireverse.commoncrawl;

public class WikiArticle {
	private String subdomain;
	private String articleName;
	
	public WikiArticle(String subdomain, String articleName) {
		this.subdomain = subdomain;
		this.articleName = articleName;
	}

	public String getSubdomain() {
		return subdomain;
	}

	public String getArticleName() {
		return articleName;
	}

	public String getKey() {
		StringBuilder key = new StringBuilder();

		key.append(subdomain);
		key.append("\t");
		
		if (articleName.endsWith("\\")) {
			key.append(articleName.toLowerCase().replaceAll("\\\\", ""));
		} else {
			key.append(articleName.toLowerCase());
		}
		
		return key.toString();
	}
}
