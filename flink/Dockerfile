FROM flink:1.18.1-scala_2.12-java11

# ---------------------------------------------------------------------------
# Jars necessarios para: Iceberg (Flink runtime + AWS/S3 bundle),
# conector Kafka e classes Hadoop usadas pelo Iceberg.
# ---------------------------------------------------------------------------
ENV ICEBERG_VERSION=1.6.1
ENV KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ENV HADOOP_UBER_VERSION=2.8.3-10.0

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -o iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar; \
    curl -fSL -o iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fSL -o flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
    curl -fSL -o flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar

# Scripts SQL e submit (montados tambem via volume; copiamos como fallback)
COPY sql /opt/sql
RUN chmod +x /opt/sql/submit_jobs.sh || true
