# Spark Connect server with S3A (hadoop-aws + the AWS SDK v2 bundle Hadoop 3.4.1 expects).
FROM apache/spark:4.0.1
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar /opt/spark/jars/
