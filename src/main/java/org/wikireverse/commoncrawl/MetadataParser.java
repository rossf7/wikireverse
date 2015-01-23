package org.wikireverse.commoncrawl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/*
 * Parses Common Crawl metadata files in WAT format. Uses the streaming method from
 * the Jackson library to parse the JSON metadata.
 * 
 * @author Ross Fairbanks
 */
public class MetadataParser {
	private static final String CONTAINER = "Container";
	private static final String ENVELOPE = "Envelope";

	private static final String PAYLOAD_METADATA = "Payload-Metadata";
	private static final String HTTP_RESPONSE_METADATA = "HTTP-Response-Metadata";
	private static final String HTML_METADATA = "HTML-Metadata";
	private static final String WARC_HEADER_METADATA = "WARC-Header-Metadata";
	private static final String WARC_DATE = "WARC-Date";
	
	private static final String HEAD = "Head";
	private static final String TITLE = "Title";
	private static final String URL = "url";
	private static final String PATH = "path";
	private static final String LINKS = "Links";

	/*
	 * Parses the metadata for links.
	 */
	public static Page parse(Page page, Text metadata, String linkType, String linkFilter)
			throws IOException, JsonParseException, URISyntaxException {
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createParser(metadata.getBytes());

		String fieldName = "";

		while (parser.nextToken() != JsonToken.END_OBJECT) {
			fieldName = parser.getCurrentName();

			if (CONTAINER.equals(fieldName)) {
				parser.skipChildren();
			}

			if (ENVELOPE.equals(fieldName)) {
				while (parser.nextToken() != JsonToken.END_OBJECT) {
					fieldName = parser.getCurrentName();
					parser.nextToken();

					if (WARC_HEADER_METADATA.equals(fieldName)) {
						page.setWarcDate(parseDate(parser));
					} else if (PAYLOAD_METADATA.equals(fieldName)) {
						
						while (parser.nextToken() != JsonToken.END_OBJECT) {
							fieldName = parser.getCurrentName();
							parser.nextToken();

							if (HTTP_RESPONSE_METADATA.equals(fieldName)) {
								
								while (parser.nextToken() != JsonToken.END_OBJECT) {
									fieldName = parser.getCurrentName();
									parser.nextToken();
									
									if (HTML_METADATA.equals(fieldName)) {
										while (parser.nextToken() != JsonToken.END_OBJECT) {
											fieldName = parser.getCurrentName();
											parser.nextToken();
											
											if (HEAD.equals(fieldName)) {
												while (parser.nextToken() != JsonToken.END_OBJECT) {
													fieldName = parser.getCurrentName();
													
													if (TITLE.equals(fieldName)) {
														page.setTitle(parser.nextTextValue());
													} else {
														parser.skipChildren();
													}
												}
											} else if (LINKS.equals(fieldName)) {
												page.setLinkUrls(parseLinks(parser, linkType, linkFilter));
											}
										}
									} else {
										parser.skipChildren();
									}
								}
							} else {
								parser.skipChildren();
							}
						}
					}
				}
			}
		}
		
		return page;
	}

	private static String parseDate(JsonParser parser) throws JsonParseException, IOException {
		String fieldName = "", date = "";
		
		while (parser.nextToken() != JsonToken.END_OBJECT) {
			fieldName = parser.getCurrentName();
		
			if (WARC_DATE.equals(fieldName)) {
				date = parser.nextTextValue().trim();
			}
		}
		
		return date;
	}
	
	private static HashSet<String> parseLinks(JsonParser parser, String linkType, String linkFilter) throws JsonParseException, IOException {
		HashSet<String> linkUrls = new HashSet<String>();
		String fieldName = "", pathValue = "", linkHref = "";

		while (parser.nextToken() != JsonToken.END_ARRAY) {
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				fieldName = parser.getCurrentName();

				if (PATH.equals(fieldName)) {
					pathValue = parser.nextTextValue();
				}

				if (URL.equals(fieldName)) {
					linkHref = parser.nextTextValue().trim();

					if (linkType.equals(pathValue) && linkHref.indexOf(linkFilter) >= 0)
						linkUrls.add(linkHref);
				}
			}
		}
		
		return linkUrls;
	}
}