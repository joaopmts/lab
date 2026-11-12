# Imagem única do cluster Spark (master + worker) com JupyterLab no master.
# A base (cópia do bitnami/spark:3.5.4) já inclui hadoop-aws, Delta 3.2.0 e Iceberg 1.6.1;
# os jars abaixo fixam explicitamente as versões usadas pelo lab.
FROM joaopmts99/bitnami-spark:3.5.4

ARG SPARK_VERSION=3.5.4
ARG SCALA_VERSION=2.12
ARG DELTA_VERSION=3.2.0
ARG ICEBERG_VERSION=1.6.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# pip/python do próprio Bitnami (/opt/bitnami/python). pyspark e delta-spark fixados
# nas versões do cluster para não puxar PySpark 4.x / delta-spark 4.x.
RUN pip install --no-cache-dir \
        "pyspark==${SPARK_VERSION}" \
        "delta-spark==${DELTA_VERSION}" \
        jupyterlab ipykernel \
        pandas pyarrow numpy scipy scikit-learn scikit-image \
        matplotlib seaborn plotly wordcloud nltk spotipy

# Formatos de tabela
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/bitnami/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar /opt/bitnami/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar /opt/bitnami/spark/jars/

# Conector Kafka (Structured Streaming)
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/bitnami/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/bitnami/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/bitnami/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/bitnami/spark/jars/

# Configurações (substituem as da imagem base, que ainda apontavam para hdfs://namenode)
COPY spark/conf/core-site.xml      /opt/bitnami/spark/conf/core-site.xml
COPY spark/conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# JupyterLab
COPY spark/conf/custom/startup.py /home/jupyter/.ipython_startup.py
ENV PYTHONSTARTUP=/home/jupyter/.ipython_startup.py \
    JUPYTER_CONFIG_DIR=/home/jupyter/.jupyter \
    JUPYTER_DATA_DIR=/home/jupyter/.local/share/jupyter
RUN mkdir -p /home/jupyter/.jupyter /home/jupyter/.local/share/jupyter /home/jupyter/notebooks \
 && chown -R 1001:1001 /home/jupyter

EXPOSE 8888
ENV SPARK_MODE=master

USER 1001

# JupyterLab sobe apenas no master; o worker roda só o processo do Spark.
CMD ["/bin/bash", "-c", "if [ \"$SPARK_MODE\" = \"master\" ]; then jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser --ServerApp.password='' --ServerApp.token='' --notebook-dir=/home/jupyter/notebooks & fi; exec /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh"]
