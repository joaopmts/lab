FROM joaopmts99/bitnami-spark:3.5.4
USER root

COPY spark/conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir jupyterlab scikit-image spotipy plotly numpy wordcloud nltk pandas pyarrow matplotlib seaborn scikit-learn scipy pyspark ipykernel delta-spark

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    /opt/bitnami/spark/jars/
RUN chmod 777 /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    /opt/bitnami/spark/jars/
    
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    /opt/bitnami/spark/jars/
RUN chmod 777 /opt/bitnami/spark/jars/delta-*.jar

# Kafka connector jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar \
    /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar \
    /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    /opt/bitnami/spark/jars/
RUN chmod 777 /opt/bitnami/spark/jars/spark-sql-kafka-*.jar \
              /opt/bitnami/spark/jars/kafka-clients-*.jar \
              /opt/bitnami/spark/jars/spark-token-provider-*.jar \
              /opt/bitnami/spark/jars/commons-pool2-*.jar

RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" > /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.local.type=hadoop" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.local.warehouse=s3a://bronze/warehouse" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.defaultCatalog=local" >> /opt/bitnami/spark/conf/spark-defaults.conf 

COPY spark/conf/custom/startup.py /home/jupyter/.ipython_startup.py

ENV PYTHONSTARTUP=/home/jupyter/.ipython_startup.py

EXPOSE 8888

ENV SPARK_MODE=master

RUN mkdir -p /home/jupyter/.jupyter && \
    mkdir -p /home/jupyter/.local/share/jupyter 

ENV JUPYTER_CONFIG_DIR=/home/jupyter/.jupyter
ENV JUPYTER_DATA_DIR=/home/jupyter/.local/share/jupyter

RUN chown -R 1001:1001 /home/jupyter

USER 1001
CMD ["/bin/bash", "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser --ServerApp.password='' --ServerApp.token='' --notebook-dir=/home/jupyter/notebooks & exec /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh"]