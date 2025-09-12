import argparse

class Args:
    @staticmethod
    def parse_args():
        p = argparse.ArgumentParser()
        p.add_argument("--input_path")
        p.add_argument("--input_format")
        p.add_argument("--temp")
        p.add_argument("--output_format")
        p.add_argument("--output_path")
        p.add_argument("--client_id")
        p.add_argument("--client_secret")
        p.add_argument("--host")
        return p.parse_args()


class Manipulation:

    @staticmethod
    def ler_csv(spark, caminho_arquivo):
        return spark.read.option("recursiveFileLookup", "true").csv(path=caminho_arquivo, header=True)

    @staticmethod
    def ler_json(spark, caminho_arquivo):
        return spark.read.option("recursiveFileLookup", "true").json(path=caminho_arquivo)

    @staticmethod
    def ler_parquet(spark, caminho_arquivo):
        return spark.read.option("recursiveFileLookup", "true").parquet(path=caminho_arquivo)

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