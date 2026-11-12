from airflow.hooks.base import BaseHook


class SparkHook(BaseHook):
    """Cria uma SparkSession (client mode) a partir da connection ``spark_default``.

    Config S3A, catálogos e jars já vêm de $SPARK_HOME/conf e $SPARK_HOME/jars na imagem
    do Airflow. Chaves ``spark.*`` no extra da connection continuam sendo aplicadas,
    caso queira sobrescrever algo.
    """

    def __init__(self, appname="AirflowSession", conn_id="spark_default"):
        super().__init__()
        self.conn_id = conn_id
        self.app_name = appname

    def _extra(self):
        conn = self.get_connection(self.conn_id)
        return conn, conn.extra_dejson or {}

    def get_confdcp(self):
        """Retorna (conf spark.* do extra, driver_class_path do extra ou None)."""
        _, extra = self._extra()
        conf = {k: v for k, v in extra.items() if k.startswith("spark.")}
        return conf, extra.get("driver_class_path")

    def get_conn(self):
        from pyspark.sql import SparkSession  # import tardio: não pesa no parse dos DAGs

        conn, _ = self._extra()
        master = f"{conn.host}:{conn.port}" if conn.port else conn.host
        conf, dcp = self.get_confdcp()

        builder = SparkSession.builder.appName(self.app_name).master(master)
        for k, v in conf.items():
            builder = builder.config(k, v)
        if dcp:
            builder = (
                builder.config("spark.driver.extraClassPath", dcp)
                .config("spark.executor.extraClassPath", dcp)
                .config("spark.jars", dcp)
            )
        return builder.getOrCreate()


# Compatibilidade com o nome antigo
sparkhook = SparkHook
