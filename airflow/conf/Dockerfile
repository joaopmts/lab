FROM apache/airflow:3.0.4

ARG AIRFLOW_VERSION=3.0.4
ARG SPARK_VERSION=3.5.4
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG DELTA_VERSION=3.2.0
ARG ICEBERG_VERSION=1.6.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH

# Spark client (mesma versão do cluster) para o SparkSubmitOperator em client mode
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Jars extras direto em $SPARK_HOME/jars: os DAGs não precisam mais passar jars/driver_class_path
RUN cd $SPARK_HOME/jars \
 && curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
 && mkdir -p /opt/airflow/conf/jars \
 && ln -s $SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/airflow/conf/jars/ \
 && ln -s $SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/airflow/conf/jars/ \
 && chown -R airflow:0 /opt/airflow/conf

# Mesma configuração S3A / catálogos do cluster
COPY spark/conf/core-site.xml       $SPARK_HOME/conf/core-site.xml
COPY spark/conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

USER airflow

# apache-airflow fixado na linha de comando para que nenhum provider faça upgrade/downgrade do core
COPY airflow/conf/requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
