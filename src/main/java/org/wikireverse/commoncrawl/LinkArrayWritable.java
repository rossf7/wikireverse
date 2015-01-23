package org.wikireverse.commoncrawl;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multisets;

/*
 * A Hadoop custom writable class that represents a collection of
 * LinkWritable objects.
 * 
 * @author Ross Fairbanks
 */
public class LinkArrayWritable extends ArrayWritable {
	
	public LinkArrayWritable() {
		super(LinkWritable.class);
	}
	
	public String getMostUsedArticleCasing() {
		HashMultiset<String> articleNames = HashMultiset.create();
		String result;

		for (Writable writable: super.get()) {
			LinkWritable link = (LinkWritable)writable;
			articleNames.add(link.getArticle().toString());
		}

		ImmutableMultiset<String> sorted = Multisets.copyHighestCountFirst(articleNames);
		result = (String)sorted.elementSet().toArray()[0];
		
		return result;
	}
}