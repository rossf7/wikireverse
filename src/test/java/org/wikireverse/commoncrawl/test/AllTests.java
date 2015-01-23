package org.wikireverse.commoncrawl.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ LinkArrayReducerTest.class,
				LinkArrayWritableTest.class,
				LinkWritableTest.class,
				MetadataParserTest.class, 
				SegmentCombinerMapperTest.class,
				WikiArticleTest.class,
				WikiMetadataTest.class,
				WikiReverseMapperTest.class})

public class AllTests {
}