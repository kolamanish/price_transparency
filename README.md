# price_transparency
Process large GB of multiline JSON file

Hospital price transparency enables Americans to have access to the cost of hospital items or services prior to receiving them. As per a Federal rule, every hospital operating in the United States must furnish easily accessible pricing information online regarding the items and services they offer. This is achieved through two means:
    1.Providing a comprehensive machine-readable file containing details of all items and services.
    2.Displaying shoppable services in a consumer-friendly format.
By providing such information, consumers will find it more convenient to compare prices across different hospitals, allowing them to estimate the expenses associated with their healthcare beforehand.

The Price Transparency files are large, multiline, and deeply nested JSON files, ranging in size from hundreds of gigabytes to terabytes. The machines currently available in the market, including those provided by cloud providers, do not possess sufficient memory capacity (in gigabytes or terabytes) to handle such massive files.

This post will guide you through the steps of preprocessing the enormous JSON file, as well as assisting in preprocessing the Price Transparency file.

The approach consists of two steps. In the first step, Python is utilized instead of Apache Spark, within the Amazon Elastic Container Service, to chunk the file. Since this is primarily a file manipulation task that does not require distributed computing, Apache Spark is not employed. The initial process involves reading the gzip file stored in the Amazon S3 bucket, passing the data to a JSON streamer, and utilizing JMESPATH, a Python package, to filter the necessary attributes for the business requirements.

The second step involves reading the chunked file from the Amazon S3 bucket, flattening the data, and storing it as a Parquet file using AWS Glue. By chunking the file within a container and subsequently calling an AWS Glue job, this approach ensures that the process does not encounter limitations imposed by AWS Glue.

Python Code to Preprocess the file :  “my-parse-large-JSON.py”
Docker Step and Docker File : Docker-Steps.txt
AWS Glue Job : my-glue-job.json
