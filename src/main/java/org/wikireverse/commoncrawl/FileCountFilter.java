package org.wikireverse.commoncrawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/*
 * A Hadoop FileSystem PathFilter used to restrict the number of files
 * parsed within a Common Crawl segment.
 * 
 * @author Ross Fairbanks
 */
public class FileCountFilter extends Configured implements PathFilter {

	private static final String MAX_FILES_KEY = "wikireverse.max.files";
	private static final int DEFAULT_MAX_FILES = 9999999;

	private static int fileCount = 0;
	private static int maxFiles = 0;

	/*
	 * Called once per file to be processed.  Returns true until max files
	 * has been reached.
	 */
	public boolean accept(Path path) {

		// If max files hasn't been set then set it to the
		// configured value.
		if (FileCountFilter.maxFiles == 0) {
			Configuration conf = getConf();
			String confValue = conf.get(MAX_FILES_KEY);

			if (confValue.length() > 0)
				FileCountFilter.maxFiles = Integer.parseInt(confValue);
			else
				FileCountFilter.maxFiles = DEFAULT_MAX_FILES;
		}

		FileCountFilter.fileCount++;

		if (FileCountFilter.fileCount > FileCountFilter.maxFiles)
			return false;

		return true;
	}
}
