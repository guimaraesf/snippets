#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: utils.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes other functions that support the other modules
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import time
from subprocess import PIPE, Popen # nosec
from pyspark.sql.functions import col, lit
from pyspark import SparkConf


class SparkUtils:
    """
    This class implements the Spark utilities.
    """
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def show_sparksession_debug(self):
        """
        Show Spark configuration.
        """
        spark_conf = SparkConf()
        self.logger.debug(f"Version of the configuration: {spark_conf.toDebugString()}")


class EnvironmentUtils:
    """
    This class provides methods to handle environment variables.
    """

    def __init__(self) -> dict:
        self.variables = {}

    def get_args(self, list_of_variables):
        """
        Returns the environment variables.
        """
        for i, name in enumerate(list_of_variables):
            if "workflow_job" in list_of_variables:
                self.variables[name] = sys.argv[i]
            else:
                self.variables[name] = sys.argv[i + 1]
        return self.variables

    def getenv(self, list_of_variables) -> dict:
        """
        Returns the environment variables.
        """
        for name in list_of_variables:
            self.variables[name] = os.getenv(name)
        return self.variables


class FileUtils:
    """
    This class provides methods to handle files.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_task_details(task):
        """
        Get the details of a task for ingestion.
        """
        task_details = [task[i] for i in range(len(task))]
        return task_details
    
    @staticmethod
    def check_blob_path(blob_path, target_date):
        """
        Checks if the blob path contains the start blob name.
        """
        start_blob_names = [
            "CNPJ",
            "SERVIDORES/ESTADUAIS/STAGING/DF/PATRIMONIO"
        ]
        if not any(blob_path.startswith(name) for name in start_blob_names):
            target_date_arg = {"target_date": target_date}
            return target_date_arg
        return {}

    @staticmethod
    def get_file_names(snippet_name, logger, target_date=None):
        """
        Returns the file name.
        """
        try:
            if target_date is not None:
                file_name = f"{target_date}_{snippet_name}"
            else:
                file_name = f"{snippet_name}"
            return file_name
        except BaseException:
            logger.error(f"Invalid file name: {file_name}")
            return None

    @staticmethod
    def delete_temporary_files(source_path, logger):
        """
        Deletes temporary files.
        """
        logger.info("Deleting temporary files.")
        try:
            for file in os.listdir(source_path):
                if any(file.endswith(ext) for ext in [".zip", ".csv"]):
                    os.remove(os.path.join(source_path, file))
        except FileNotFoundError as exception:
            logger.error(f"File not found: {exception}")


class SqlUtils:
    """
    Runs a SQL query on the given DataFrame.
    """

    def __init__(self):
        pass

    def run_select(self, dataframe, query):
        """
        Runs a SQL query on the given DataFrame.
        """
        result_dataframe = dataframe.select(query)
        return result_dataframe


class HdfsUtils:
    """
    Runs a subprocess.
    """

    def __init__(self, commands, popen_hadoop=Popen, pipe=PIPE) -> None:
        self.commands = commands
        self.popen_hadoop = popen_hadoop
        self.pipe = pipe

    def run_subprocess(self):
        """
        Runs a subprocess.
        """
        process = self.popen_hadoop(self.commands, stdin=self.pipe, bufsize=-1)
        process.communicate()


class DataframeUtils:
    """
    Utilities for working with DataFrames.
    """

    def __init__(self, spark, file_name, schema, logger):
        self.spark = spark
        self.file_name = file_name
        self.schema = schema
        self.logger = logger
        self.delimiter = ";"
        self.encoding = "ISO-8859-1"
        self.header = True

    def read_dataframe(self):
        """
        Retrieves a DataFrame from a file.
        """
        self.logger.info("Reading the downloaded CSV file")
        dataframe = (
            self.spark.read.option("delimiter", self.delimiter)
            .option("encoding", self.encoding)
            .csv(self.file_name, header=self.header, schema=self.schema)
        )
        return dataframe

    def fixing_rows(self, df):
        """
        Concatenates pairs of rows where the value of the column "valor_situacao_parcela"
        is missing (NULL) with the next non-NULL row.

        Args:
            spark (SparkSession): a SparkSession object.
            df (pyspark.sql.DataFrame): a PySpark DataFrame to process.

        Returns:
            a new DataFrame with pairs of rows concatenated.
        """
        qtd_de_linhas_com_quebras = df.where(
            col("VALOR_SITUACAO_PARCELA").isNull()
        ).count()

        if qtd_de_linhas_com_quebras > 0:
            self.logger.info("Fixing rows breaks")

            df_padrao = df.where(col("VALOR_SITUACAO_PARCELA").isNull())
            df_complementar = df.where(col("VALOR_SITUACAO_PARCELA").isNotNull())

            rows_as_lists = []

            # Percorrer cada linha do DataFrame do PySpark
            for row in df_padrao.rdd.collect():
                # Adicionar cada linha como uma lista na lista externa
                rows_as_lists.append(list(row))

            # Inicializar uma lista vazia para armazenar os pares concatenados
            result = []

            # Iterar sobre a lista em incrementos de 2
            for i in range(0, len(rows_as_lists), 2):
                # Concatenar os valores em pares e adicionar a lista de resultados
                pair_none = rows_as_lists[i] + rows_as_lists[i + 1]
                # Remover None
                pair = [x for x in pair_none if x is not None]
                result.append(pair)

            schema = [
                "A1",
                "A2",
                "A3",
                "A4",
                "A5",
                "A6",
                "A7",
                "A8",
                "A9",
                "A10",
                "A11",
                "A12",
                "A13",
                "A14",
                "A15",
                "A16",
                "A17",
            ]

            df_no_rename = self.spark.createDataFrame(result, schema)

            df_to_union = (
                df_no_rename.withColumn("DATA_REQUERIMENTO", col("A1"))
                .withColumn("NUMERO_REQUERIMENTO", col("A2"))
                .withColumn("CPF", col("A3"))
                .withColumn("PIS_PASEP_NIT", col("A4"))
                .withColumn("RGP", col("A5"))
                .withColumn("DEFESO", col("A6") + col("A7"))
                .withColumn("DATA_INICIO_DEFESO", col("A8"))
                .withColumn("DATA_FIM_DEFESO", col("A9"))
                .withColumn("NOME_REQUERENTE", col("A10"))
                .withColumn("UF_RECEPCAO", col("A11"))
                .withColumn("MUNICIPIO_RECEPCAO", col("A12"))
                .withColumn("NUMERO_PARCELA", col("A13"))
                .withColumn("DATA_EMISSAO_PARCELA", col("A14"))
                .withColumn("DATA_SITUACAO_PARCELA", col("A15"))
                .withColumn("SITUACAO_PARCELA", col("A16"))
                .withColumn("VALOR_SITUACAO_PARCELA", col("A17"))
                .withColumn("ARQUIVO", lit(None))
                .withColumn("DATA_PROCESSAMENTO", lit(None))
                .drop(
                    "A1",
                    "A2",
                    "A3",
                    "A4",
                    "A5",
                    "A6",
                    "A7",
                    "A8",
                    "A9",
                    "A10",
                    "A11",
                    "A12",
                    "A13",
                    "A14",
                    "A15",
                    "A16",
                    "A17",
                )
            )

            df_completo = df_complementar.union(df_to_union)

            return df_completo

        else:
            return df


class Stopwatch:
    """
    Create a Stopwatch.
    """
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self):
        """
        Start the Stopwatch.
        """
        self.start_time = time.time()

    def stop(self):
        """
        Stop the Stopwatch.
        """        
        self.end_time = time.time()

    def elapsed_time(self, logger):
        """
        Return the elapsed time.
        """
        elapsed_time = self.end_time - self.start_time

        hours = round(elapsed_time // 3600, 2)
        minutes = round((elapsed_time % 3600) // 60, 2)
        seconds = round(elapsed_time % 60, 4)
    
        logger.info(f"Execution time: {hours} hours {minutes} minutes {seconds} seconds")
