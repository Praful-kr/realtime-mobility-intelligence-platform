FROM apache/spark:3.5.3-python3
USER root
ARG MVN=https://repo1.maven.org/maven2
# Jars pinned to Spark 3.5.3 / Hadoop 3.3.4 / Delta 3.2.1 and baked in (no runtime downloads).
RUN set -eux; cd /opt/spark/jars; \
  for u in \
    org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar \
    org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ; do \
    curl -fsSL -O "$MVN/$u"; done
RUN pip install --no-cache-dir --no-deps delta-spark==3.2.1 importlib_metadata zipp prometheus-client==0.21.0 && ls /opt/spark/jars | grep -c -E "delta|kafka|hadoop-aws|aws-java|avro"
USER spark
