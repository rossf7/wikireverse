# wikireverse
Hadoop jobs for WikiReverse project. Parses Common Crawl data for links to Wikipedia articles. Launched using the [elasticrawl](https://github.com/rossf7/elasticrawl) CLI tool.

# Running using Elasticrawl

Here is how to configure Elasticrawl and an example of parsing some data. To run this example you need an AWS account and it
will cost between 40 and 80 cents.

* Install elasticrawl using the deploy packages for OS X or Linux. [deploy steps](https://github.com/rossf7/elasticrawl#installation)
* Run the init command and choose an S3 bucket for storing your data and logs.

```
$ ./elasticrawl init wikireverse-2014-52
Enter AWS Access Key ID:
Enter AWS Secret Access Key: 
…

Bucket s3://wikireverse-2014-52 created
Config dir /home/vagrant/.elasticrawl created
Config complete
```

* Edit ~/.elasticrawl/jobs.yml and replace the steps section with the WikiReverse settings.

```
steps:
  parse:
    jar: 's3://wikireverse/jar/wikireverse-0.0.1.jar'
    class: 'org.wikireverse.commoncrawl.WikiReverse'
    input_filter: 'wat/*.warc.wat.gz'
    emr_config: #'s3://wikireverse/jar/parse-mapred-site.xml'
  combine:
    jar: 's3://wikireverse/jar/wikireverse-0.0.1.jar'
    class: 'org.wikireverse.commoncrawl.SegmentCombiner'
    input_filter: 'part-*'
    emr_config: #'s3://wikireverse/jar/combine-mapred-site.xml'
```

* Run a parse job to process the first 2 files in the first 2 segments.

```
$ ./elasticrawl parse CC-MAIN-2014-52 --max-segments 2 --max-files 2
Segments
Segment: 1418802765002.8 Files: 176
Segment: 1418802765093.40 Files: 176

Job configuration
Crawl: CC-MAIN-2014-52 Segments: 2 Parsing: 2 files per segment

Cluster configuration
Master: 1 m1.medium  (Spot: 0.12)
Core:   2 m1.medium  (Spot: 0.12)
Task:   --
Launch job? (y/n)
y

Job: 1422436508058 Job Flow ID: j-2KMT57YJN4EJA
```

* Run a combine job to combine the results from the parse job.

```
$ ./elasticrawl combine --input-jobs 1422436508058
No entry for terminal type "xterm";
using dumb terminal settings.
Job configuration
Combining: 2 segments

Cluster configuration
Master: 1 m1.medium  (Spot: 0.12)
Core:   2 m1.medium  (Spot: 0.12)
Task:   --
Launch job? (y/n)
y

Job: 1422438064880 Job Flow ID: j-1A6Q7LJ1G9TX
```

* Finally run an output job to produce the results. This job is launched manually in the EMR section of the AWS Console.
The job step takes in 3 arguments.
** Class:             org.wikireverse.commoncrawl.OutputToText
** Input Location:    e.g. s3://wikireverse-2014-52/data/2-combine/1422438064880/part-*
** Output Location:   e.g. s3://wikireverse-2014-52/data/3-output/2014-52-test/

```
JAR Location: s3://wikireverse/jar/wikireverse-0.0.1.jar
Arguments: org.wikireverse.commoncrawl.OutputToText s3://wikireverse-2014-52/data/2-combine/1422438064880/part-* s3://wikireverse-2014-52/data/3-output/2014-52-test/
```

* The final results can be downloaded from the S3 section of the AWS Console.
* Running the destroy command will clean up and delete your S3 bucket. Otherwise you will be charged for any data stored in your S3 bucket.

```
./elasticrawl destroy
WARNING:
Bucket s3://wikireverse-2014-52 and its data will be deleted
Config dir /Users/ross/.elasticrawl will be deleted
Delete? (y/n)
y

Bucket s3://wikireverse-2014-52 deleted
Config dir /Users/ross/.elasticrawl deleted
Config deleted
```

## TODO

* Add more detail on building the code using Maven and Eclipse.

## Thanks

* Thanks to everyone at Common Crawl for making this awesome dataset available.
* Thanks to Wikipedia for having such interesting data.

## License

This code is licensed under the MIT license.
