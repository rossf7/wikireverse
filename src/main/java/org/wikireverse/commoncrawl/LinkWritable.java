package org.wikireverse.commoncrawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * A Hadoop custom writable class that represents a link to
 * a Wikipedia article.
 * 
 * @author Ross Fairbanks
 */
public class LinkWritable implements Writable {
	private Text article;
	private Text title;
	private Text date;
	private Text url;
	
	public LinkWritable() {
		set(new Text(), new Text(), new Text(), new Text());
	}

	public LinkWritable(String article, String title, String date, String url) {
		set(new Text(article), new Text(title), new Text(date), new Text(url));
	}

	public LinkWritable(Text article, Text title, Text date, Text url) {
		set(article, title, date, url);
	}
	
	public void set (Text article, Text title, Text date, Text url) {
		this.article = article;
		this.title = title;
		this.date = date;
		this.url = url;
	}

	public Text getArticle() {
		return article;
	}

	public Text getTitle() {
		return title;
	}

	public Text getDate() {
		return date;
	}

	public Text getUrl() {
		return url;
	}

	public void readFields(DataInput in) throws IOException {
		article.readFields(in);
		title.readFields(in);
		date.readFields(in);
		url.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		article.write(out);
		title.write(out);
		date.write(out);
		url.write(out);
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		String hostName = "";

		output.append(date.toString());
		output.append("\t");

		if (title.toString().endsWith("\\")) {
			output.append(title.toString().replaceAll("\\\\", ""));
		} else {
			output.append(title.toString());
		}

		try {
			URI pageUri = new URI(url.toString());
			hostName = pageUri.getHost().toLowerCase();
		} catch (URISyntaxException e) {
			// consume exception
		}
		
		output.append("\t");
		output.append(hostName);	
		output.append("\t");
		output.append(url.toString());	
		
		return output.toString();
	}
}