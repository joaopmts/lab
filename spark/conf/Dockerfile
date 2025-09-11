FROM bitnami/spark:3.5.4
USER root

COPY spark/conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir jupyterlab numpy wordcloud nltk pandas pyarrow matplotlib seaborn scikit-learn scipy pyspark ipykernel

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