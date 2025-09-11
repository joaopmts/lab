from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession

class sparkhook(BaseHook):
    def __init__(self, appname="AirflowSession"):
        super().__init__()
        self.conn_id = "spark_default"
        self.app_name = appname

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson

        master = f"{conn.host}:{conn.port}"
        conf = {k: v for k, v in extra.items() if k.startswith("spark.")}
        appname = self.app_name
        dcp = extra.get("driver_class_path")

        spark  = (
            SparkSession.builder
            .appName(appname)
            .master(master)
        )

        for k, v in conf.items():
            spark = spark.config(k, v)

        spark = (spark.config("spark.driver.extraClassPath", dcp) 
                    .config("spark.executor.extraClassPath", dcp) 
                    .config("spark.jars", dcp))
        
        return spark.getOrCreate()
    
    def get_confdcp(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson
        conf = {k: v for k, v in extra.items() if k.startswith("spark.")}
        dcp = extra.get("driver_class_path")
        return conf, dcp
        