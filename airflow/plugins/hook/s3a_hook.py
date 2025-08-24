from airflow.hooks.base import BaseHook

class S3AHook(BaseHook):
    def __init__(self):
        super().__init__()
        self.conn_id = "s3a_default"

    def get_conn(self):
        conn = self.get_connection(self.conn_id)

        extra = conn.extra_dejson
        endpoint_url = f"http://{conn.host}:{conn.port}"
        jar_home = extra.get("jar_home")
        hadoop_aws = extra.get("hadoop_aws")
        aws_sdk_bundle = extra.get("aws_sdk_bundle")

        s3a_conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint_url,
        "spark.hadoop.fs.s3a.access.key": conn.login,
        "spark.hadoop.fs.s3a.secret.key": conn.password,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

        driver_class_path=f'{jar_home}/hadoop-aws-{hadoop_aws}.jar,{jar_home}/aws-java-sdk-bundle-{aws_sdk_bundle}.jar'

        return s3a_conf, driver_class_path
    