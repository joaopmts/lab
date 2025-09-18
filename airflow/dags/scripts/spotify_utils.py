import argparse

class Manipulation:

    @staticmethod
    def ler_csv(spark, caminho_arquivo):
        return spark.read.csv(path=caminho_arquivo, header=True)

    @staticmethod
    def ler_json(spark, caminho_arquivo):
        return spark.read.json(path=caminho_arquivo)

    @staticmethod
    def ler_parquet(spark, caminho_arquivo):
        return spark.read.parquet(path=caminho_arquivo)

    @staticmethod
    def read_by_ext(spark, srcformat, src):
        if srcformat == "json":
            return Manipulation.ler_json(spark, src)
        elif srcformat == "csv":
            return Manipulation.ler_csv(spark, src)
        else:
            return Manipulation.ler_parquet(spark, src)

    @staticmethod
    def salvar_csv(df, final_file, sep=";", mode="overwrite"):
        df.write.mode(mode).option("sep", sep).option("header", True).csv(final_file)

    @staticmethod
    def salvar_json(df, final_file, mode="overwrite"):
        df.write.mode(mode).json(final_file)

    @staticmethod
    def salvar_parquet(df, final_file, mode="overwrite"):
        df.write.mode(mode).parquet(final_file)

    @staticmethod
    def write_by_ext(df, file_format, final_path):
        if file_format == "json":
            Manipulation.salvar_json(df, final_path)
        elif file_format == "csv":
            Manipulation.salvar_csv(df, final_path)
        else:
            Manipulation.salvar_parquet(df, final_path)