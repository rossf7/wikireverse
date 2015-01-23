package org.wikireverse.commoncrawl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/*
 * Wikipedia metadata is loaded from a JSON file. It is used to exclude non-article pages
 * such as the Wikipedia home page or talk pages. 
 * 
 * the Jackson library to parse the JSON metadata.
 * 
 * @author Ross Fairbanks
 */
public class WikiMetadata {
	private static final String COUNTER_GROUP = "Wiki Metadata Counters";
	private static final String SKIP_URI_SYNTAX_EXCEPTION = "Skipped - URI syntax exception";
	private static final String SKIP_NON_ARTICLE_URL = "Skipped - Non-Article URL";
	private static final String SKIP_WIKIPEDIA_PAGE = "Skipped - Wikipedia Page";
	private static final String SKIP_WIKIMEDIA_PAGE = "Skipped - Wikimedia Page";

	private static final String WIKIPEDIA_DOMAIN = ".wikipedia.org";
	private static final String WIKIMEDIA_DOMAIN = ".wikimedia.org";
	private static final String WIKIPEDIA_PATH = "/wiki/";

	private static final String EN_SUBDOMAIN = "en";
	private static final String WIKIPEDIAS = "wikipedias";
	private static final String SUBDOMAIN = "subdomain";
	private static final String WIKI_METADATA_JSON = "wiki_metadata.json";
	private static final String MAIN_PAGE = "main_page";
	private static final String NAMESPACES = "namespaces";

	private static final int TITLE_LENGTH = 70;

	private HashSet<String> wikiSubdomains = new HashSet<String>();
	private HashMap<String, String> wikiMainPages = new HashMap<String, String>();
	private HashMap<String, HashSet<String>> wikiNamespaces = new HashMap<String, HashSet<String>>();

	private static final Logger LOG = Logger.getLogger(WikiMetadata.class);

	/*
	 * Load Wikipedia metadata from JSON resource file.
	 */
	public WikiMetadata() throws IOException, JsonParseException {
		loadWikiMetadata();
	}
	
	/*
	 * Checks if this is a Wikipedia or Wikimedia page. If so this is an internal link
	 * and will not be counted.
	 */	
	public boolean isWikiPage(String url, Reporter reporter) throws URISyntaxException {
		boolean result = false;
		
		if (url != null && (url.indexOf(WIKIPEDIA_DOMAIN) >= 0 ||
				url.indexOf(WIKIMEDIA_DOMAIN) >= 0)) {
			URI pageUri = new URI(url);
			String pageHost = pageUri.getHost();
			
			if (pageHost != null && pageHost.endsWith(WIKIPEDIA_DOMAIN)) {
				LOG.info(url);
				reporter.incrCounter(COUNTER_GROUP, SKIP_WIKIPEDIA_PAGE, 1);
				result = true;
			}

			if (pageHost != null && pageHost.endsWith(WIKIMEDIA_DOMAIN)) {
				LOG.info(url);
				reporter.incrCounter(COUNTER_GROUP, SKIP_WIKIMEDIA_PAGE, 1);
				result = true;
			}
		}
		
		return result;
	}

	/*
	 * Filter Wikipedia articles from a list of Wikipedia URLs. A WikiArticle result
	 * is created for each valid article link.
	 */
	public Hashtable<String, LinkWritable> createResults(Page page, Reporter reporter)
			throws IOException, JsonParseException, URISyntaxException {
		Hashtable<String, LinkWritable> results = new Hashtable<String, LinkWritable>();
		HashSet<String> linkUrls = page.getLinkUrls();
		
		if (linkUrls != null && linkUrls.isEmpty() == false) {
			List<WikiArticle> articles = filterArticles(linkUrls, reporter);

			for (WikiArticle article : articles) {
			results.put(article.getKey(), new LinkWritable(article.getArticleName(),
															formatField(page.getTitle(), TITLE_LENGTH),
															page.getWarcDate(),
															page.getUrl()));
			}
		}
		
		return results;
	}
	
	/*
	 * Filter Wikipedia articles from a list of Wikipedia URLs.
	 * URLs are excluded if they are from an invalid subdomain or
	 * the page has a namespace such as talk.
	 */
	private List<WikiArticle> filterArticles(HashSet<String> wikiUrls, Reporter reporter) {
		List<WikiArticle> articles = new ArrayList<WikiArticle>();
		URI wikiUri;
		String host = "", path = "", subdomain = "", articleName = "";
		
		for (String wikiUrl : wikiUrls) {
			try {
				wikiUri = new URI(wikiUrl);
				host = wikiUri.getHost();
				path = wikiUri.getPath();

				if (host != null && host.endsWith(WIKIPEDIA_DOMAIN)) {
					subdomain = host.replace(WIKIPEDIA_DOMAIN, "");
					subdomain = subdomain.toLowerCase();

					if (path != null && path.startsWith(WIKIPEDIA_PATH)) {
						articleName = path.replace(WIKIPEDIA_PATH, "");
						articleName = articleName.replaceAll("(\r\n|\r|\n)", "");
						articleName = StringEscapeUtils.unescapeHtml(articleName);
					} else {
						articleName = "";
					}
						
					if (isValidSubdomain(subdomain)
							&& articleName.length() > 0
							&& isArticle(subdomain, articleName)) {
						
						articles.add(new WikiArticle(subdomain, articleName));
					} else {
						reporter.incrCounter(COUNTER_GROUP, SKIP_NON_ARTICLE_URL, 1);
					}
				}
			} catch (URISyntaxException us) {
				reporter.incrCounter(COUNTER_GROUP, SKIP_URI_SYNTAX_EXCEPTION, 1);
			}
		}

		return articles;
	}
	
	private boolean isValidSubdomain(String subdomain) {
		return wikiSubdomains.contains(subdomain);
	}

	private boolean isArticle(String subdomain, String path) {
		if (isMainPage(subdomain, path) == false 
				&& pageHasNamespace(subdomain, path) == false
				&& pageHasNamespace(EN_SUBDOMAIN, path) == false)
			return true;
		else
			return false;
	}

	private boolean isMainPage(String subdomain, String path) {
		String wikiMainPage = wikiMainPages.get(subdomain);
		if (path.equals(wikiMainPage))
			return true;
		else
			return false;
	}

	private boolean pageHasNamespace(String subdomain, String path) {
		if (path.contains(":")) {
			String namespace = path.split(":")[0];
			HashSet<String> namespaces = wikiNamespaces.get(subdomain);

			if (namespaces != null && namespaces.contains(namespace))
				return true;
			else
				return false;
		} else
			return false;
	}
	
	private void loadWikiMetadata() throws IOException, JsonParseException {
		InputStream in = MetadataParser.class
				.getResourceAsStream(WIKI_METADATA_JSON);
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createParser(in);

		String fieldName = "", subdomain = "";

		while (parser.nextToken() != JsonToken.END_OBJECT) {
			fieldName = parser.getCurrentName();

			if (WIKIPEDIAS.equals(fieldName)) {
				while (parser.nextToken() != JsonToken.END_ARRAY) {
					while (parser.nextToken() != JsonToken.END_OBJECT) {
						fieldName = parser.getCurrentName();

						if (SUBDOMAIN.equals(fieldName)) {
							subdomain = parser.nextTextValue();
							wikiSubdomains.add(subdomain);
						}

						if (MAIN_PAGE.equals(fieldName)) {
							wikiMainPages.put(subdomain, parser.nextTextValue());
						}

						if (NAMESPACES.equals(fieldName)) {
							HashSet<String> namespaces = new HashSet<String>();
							parser.nextToken();

							while (parser.nextToken() != JsonToken.END_OBJECT) {
								namespaces.add(parser.getCurrentName());
								parser.nextToken();
							}

							wikiNamespaces.put(subdomain, namespaces);
						}
					}
				}
			}
		}
	}
	
	private String formatField(String value, int fieldLength) {
		if (value != null) {
			// Normalise whitespace to single spaces
			value = value.replaceAll("\\s+", " ");
	
			// Remove carriage returns and newlines
			value = value.replaceAll("(\r\n|\r|\n)", "");
	
			// Decode HTML entities
			value = StringEscapeUtils.unescapeHtml(value);
			value = truncateField(value, fieldLength);
		} else value = "";
		
		return value;
	}

	private String truncateField(String value, int fieldLength) {
		if (value.length() > fieldLength) {
			value = value.substring(0, fieldLength); 
		}

		return value;
	}
}