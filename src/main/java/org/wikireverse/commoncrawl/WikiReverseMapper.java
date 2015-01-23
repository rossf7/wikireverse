package org.wikireverse.commoncrawl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;

import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

import org.wikireverse.commoncrawl.MetadataParser;
import org.wikireverse.commoncrawl.LinkArrayWritable;

/*
 * A Hadoop custom mapper that parses Common Crawl metadata files
 * looking for reverse web-links to Wikipedia articles.
 * 
 * @author Ross Fairbanks
 */
public class WikiReverseMapper extends MapReduceBase
	implements MapRunnable<LongWritable, WritableWarcRecord, Text, LinkArrayWritable> {
	
	private static final Logger LOG = Logger.getLogger(WikiReverseMapper.class);

	private static final String WARC_TARGET_URI = "WARC-Target-URI";
	
	private static final String WIKIPEDIA_DOMAIN = ".wikipedia.org";
	private static final String LINK_TYPE = "A@/href";
	
	// Constants for mapper specific counters.
	private static final String COUNTER_GROUP = "WikiReverse Mapper Counters";
	private static final String URLS_PARSED = "URLs Parsed";
	private static final String RECORDS_FETCHED = "Records Fetched";
	
	private static final String MAPPER_INTERRUPTED = "Mapper Interrupted";
	private static final String RUN_IO_EXCEPTION = "Run IO exception";
	private static final String RUN_EXCEPTION = "Run - Other exception";
	
	private static final String URI_SYNTAX_EXCEPTION = "URI syntax exception";
	private static final String JSON_PARSE_EXCEPTION = "JSON parse exception";
	private static final String MAP_IO_EXCEPTION = "Map IO exception";
	private static final String MAP_EXCEPTION = "Map - Other exception";

	/*
	 * Run method is called once per Common Crawl WAT file. Wikipedia metadata
	 * is loaded and the map method is called once per page in the metadata file.
	 */
	public void run(RecordReader<LongWritable, WritableWarcRecord> input,
					OutputCollector<Text, LinkArrayWritable> output, Reporter reporter)
					throws IOException {
		try {
			WikiMetadata wikiMetadata = new WikiMetadata();
			
			LongWritable key = input.createKey();
			WritableWarcRecord value = input.createValue();
			
			while (input.next(key, value)) {
				map(key, value, output, reporter, wikiMetadata);
				reporter.incrCounter(COUNTER_GROUP, RECORDS_FETCHED, 1);
			}
			
		} catch(InterruptedException ie) {
			reporter.incrCounter(COUNTER_GROUP, MAPPER_INTERRUPTED, 1);
			LOG.error(StringUtils.stringifyException(ie));
		} catch(IOException io) {
			reporter.incrCounter(COUNTER_GROUP, RUN_IO_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(io));
		} catch(Exception e) {
			reporter.incrCounter(COUNTER_GROUP, RUN_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(e));
		} finally {
			input.close();
		}
	}
	
	/*
	 * Map method parses the metadata for a page appearing in the Common Crawl. Checks if the JSON contains wikipedia.org
	 * and if so parses it for links. A LinkWritable result is written for each link that is found.
	 */
	public void map(LongWritable inputKey, WritableWarcRecord inputValue, OutputCollector<Text, LinkArrayWritable> output,
			Reporter reporter, WikiMetadata wikiMetadata)
			throws IOException, InterruptedException {

		try {
			// Get Warc record from the writable wrapper.
			WarcRecord record = inputValue.getRecord();
			String url = record.getHeaderMetadataItem(WARC_TARGET_URI);

			if (wikiMetadata.isWikiPage(url, reporter) == false) {
				Text metadata = new Text(record.getContent());
			
				if (metadata.find(WIKIPEDIA_DOMAIN) >= 0) {
					Page page = new Page(url);
					page = MetadataParser.parse(page, metadata, LINK_TYPE, WIKIPEDIA_DOMAIN);
					Hashtable<String, LinkWritable> results = wikiMetadata.createResults(page, reporter);

					if (results != null && results.isEmpty() == false) {
						Text outputKey = new Text();
						LinkArrayWritable outputValue = new LinkArrayWritable();						
						LinkWritable[] linkArray = new LinkWritable[1];

						for(String key : results.keySet()) {
							linkArray[0] = results.get(key);

							outputKey.set(key);
							outputValue.set(linkArray);
							
							output.collect(outputKey, outputValue);
						}

						reporter.incrCounter(COUNTER_GROUP, URLS_PARSED, results.size());
					}
				}
			}
			
		} catch (URISyntaxException us) {
			reporter.incrCounter(COUNTER_GROUP, URI_SYNTAX_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(us));
		} catch (JsonParseException jp) {
			reporter.incrCounter(COUNTER_GROUP, JSON_PARSE_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(jp));
		} catch (IOException io) {
			reporter.incrCounter(COUNTER_GROUP, MAP_IO_EXCEPTION, 1);
			LOG.error(StringUtils.stringifyException(io));
		} catch (Exception e) {
			try {
				reporter.incrCounter(COUNTER_GROUP, MAP_EXCEPTION, 1);
				LOG.error(StringUtils.stringifyException(e));
			} catch (Exception ie) {
				// Log and consume inner exceptions when logging.
				LOG.error(ie.toString());
			}
		}
	}
}