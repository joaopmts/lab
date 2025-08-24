FROM bitnami/spark:3.5.4
USER root

COPY spark/conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir jupyter notebook pyspark && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

EXPOSE 8888

ENV SPARK_MODE=master
USER 1001
CMD ["/bin/bash", "-c", "jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --no-browser --NotebookApp.password='' --NotebookApp.token='' --notebook-dir=/home/jupyter/notebooks & exec /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh"]
