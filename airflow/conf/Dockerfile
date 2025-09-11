
FROM apache/airflow:3.0.4

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

ENV JARS_DIR=/opt/airflow/conf/jars

RUN mkdir -p "$JARS_DIR" \
 && curl -fsSL -o "$JARS_DIR/hadoop-aws-3.3.4.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
 && curl -fsSL -o "$JARS_DIR/aws-java-sdk-bundle-1.12.262.jar" \
      "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
 && chown -R airflow:0 /opt/airflow/conf
 
USER airflow


COPY airflow/conf/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt