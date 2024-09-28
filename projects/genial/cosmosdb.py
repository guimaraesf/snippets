import logging
import sys
from datetime import datetime

from azure.cosmos import CosmosClient
from azure.cosmos.container import ContainerProxy
from azure.cosmos.database import DatabaseProxy
from azure.cosmos.errors import HttpResponseError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import *

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Captacao:
    def __init__(
        self,
        spark: SparkSession,
        current_date: str,
        execution_date: str,
        cfg_cosmosdb: dict,
    ) -> None:
        self.spark = spark
        self.current_date = current_date
        self.execution_date = execution_date
        self._cfg_cosmosdb = cfg_cosmosdb

    @property
    def account_endpoint(self):
        """
        The account endpoint for the Cosmos DB instance.

        Returns:
            str: The account endpoint URL as a string.
        """
        return self._cfg_cosmosdb["account_endpoint"]

    @property
    def account_key(self):
        """
        The account key for the Cosmos DB instance.

        Returns:
            str: The account key as a string.
        """
        return self._cfg_cosmosdb["account_key"]

    @property
    def database(self):
        """
        The database name in the Cosmos DB instance.

        Returns:
            str: The database name as a string.
        """
        return self._cfg_cosmosdb["database"]

    @property
    def container(self):
        """
        The container name in the Cosmos DB instance.

        Returns:
            str: The container name as a string.
        """
        return self._cfg_cosmosdb["container"]

    @property
    def write_strategy(self):
        """
        The write strategy for the Cosmos DB instance.

        Returns:
            str: The write strategy as a string.
        """
        return self._cfg_cosmosdb["write_strategy"]

    def read_captacao_table(self, table_name: str) -> DataFrame:
        """
        Reads a table from Spark and applies various transformations to it.

        Args:
            table_name (str): The name of the table to be read from Spark.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        df = self.spark.table(table_name)
        max_date = df.agg(fn.max("DT_PARTITION")).collect()[0][0]
        date_base = fn.to_date(fn.col("DT_BASE"), "YYYY-MM-DD")
        # Set DT_PARTITION mais recente da origem
        df = df.filter((fn.col("DT_PARTITION") == max_date))
        df = df.select(
            fn.col("ID_TRANSACAO"),
            fn.col("ID_CLIENTE"),
            fn.col("CD_CONTA"),
            fn.col("CPF_CNPJ"),
            fn.col("CLIENTE"),
            fn.col("ID_ASSESSOR"),
            fn.col("ASSESSOR"),
            fn.col("FILIAL"),
            fn.col("GRUPO"),
            fn.col("COD_ASSESSOR_SINACOR"),
            fn.col("VALOR"),
            fn.col("TIPO"),
            fn.col("FLUXO"),
            fn.col("DT_BASE"),
        )
        # Set 'DT_PARTITION' do processo que está sendo executado
        # E 'DT_PARTITION' = 'DT_BASE' impede que ocorra duplicidade de um mesmo 'id' para 'DT_PARTITION' diferente
        df = df.withColumn("DT_PARTITION", date_base)
        df = df.withColumn("DT_BASE", date_base)
        # Esses filtros são necessários porque o CosmosDB não aceita valores nulos.
        df = df.filter(
            (fn.col("DT_BASE") >= self.execution_date)
            & (fn.col("DT_BASE") <= self.current_date)
            & (fn.col("ID_CLIENTE").isNotNull())
            & (fn.col("CD_CONTA").isNotNull())
            & (fn.col("FLUXO").isNotNull())
            & (fn.col("VALOR").isNotNull())
            & (fn.col("DT_BASE").isNotNull())
        )
        return df

    @staticmethod
    def create_primary_key_column(df: DataFrame) -> DataFrame:
        """
        Creates a primary key column in the DataFrame.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The DataFrame with the new 'id' column.
        """
        df = df.withColumn(
            "id",
            fn.when(
                fn.col("ID_TRANSACAO") == "",
                fn.concat(
                    fn.col("ID_CLIENTE"),
                    fn.lit("_"),
                    fn.col("FLUXO"),
                    fn.lit("_"),
                    fn.col("VALOR"),
                    fn.lit("_"),
                    fn.to_date(fn.col("DT_BASE"), "YYYY-MM-DD"),
                    fn.lit("_"),
                    fn.col("TIPO"),
                    fn.lit("_"),
                    fn.col("CD_CONTA"),
                ),
            ).otherwise(
                fn.concat(fn.col("ID_CLIENTE"), fn.lit("_"), fn.col("ID_TRANSACAO"))
            ),
        ).drop("ID_TRANSACAO")
        df = df.filter((fn.col("id").isNotNull()))
        return df

    @staticmethod
    def select_and_types_data(df: DataFrame) -> DataFrame:
        """
        Selects and casts the columns of a DataFrame to specific types.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The DataFrame with the selected and casted columns.
        """
        df = df.select(
            fn.col("id").cast(StringType()),
            fn.col("DT_PARTITION").cast(StringType()),
            fn.col("ID_CLIENTE").cast(StringType()),
            fn.col("CD_CONTA").cast(StringType()),
            fn.col("CPF_CNPJ").cast(StringType()),
            fn.col("CLIENTE").cast(StringType()),
            fn.col("ID_ASSESSOR").cast(IntegerType()),
            fn.col("ASSESSOR").cast(StringType()),
            fn.col("FILIAL").cast(StringType()),
            fn.col("GRUPO").cast(StringType()),
            fn.col("COD_ASSESSOR_SINACOR").cast(IntegerType()),
            fn.col("VALOR").cast(DoubleType()),
            fn.col("TIPO").cast(StringType()),
            fn.col("FLUXO").cast(StringType()),
            fn.col("DT_BASE").cast(StringType()),
        )
        return df

    def instantiate_cosmos_client(self) -> CosmosClient:
        """
        Instantiates a CosmosClient using the account endpoint and account key.

        Returns:
            CosmosClient: The instantiated CosmosClient.

        Raises:
            HttpResponseError: If there is an error in the HTTP response.
        """
        try:
            client = CosmosClient(
                url=self.account_endpoint, credential=self.account_key
            )
            return client
        except HttpResponseError as e:
            raise e

    def get_database(self, client) -> DatabaseProxy:
        """
        Gets a DatabaseProxy client for the database.

        Args:
            client (CosmosClient): The CosmosClient.

        Returns:
            DatabaseProxy: The DatabaseProxy client for the database.
        """
        return client.get_database_client(self.database)

    def get_container(self, database) -> ContainerProxy:
        """
        Gets a ContainerProxy client for the container.

        Args:
            database (DatabaseProxy): The DatabaseProxy client for the database.

        Returns:
            ContainerProxy: The ContainerProxy client for the container.
        """
        return database.get_container_client(self.container)

    @staticmethod
    def query_items(query: str, container: ContainerProxy) -> list[dict]:
        """
        Queries items from the container.

        Args:
            query (str): The query string.

        Returns:
            list[dict]: The list of queried items.
        """
        return list(
            container.query_items(query=query, enable_cross_partition_query=True)
        )

    @staticmethod
    def filter_non_matching_records(dfs: list[DataFrame]) -> DataFrame:
        """
        Filters out records from the first DataFrame in the list that have a match
        in the second DataFrame based on 'id' and 'DT_BASE'.

        Args:
            dfs (list[DataFrame]): A list containing two DataFrames.

        Returns:
            DataFrame: A DataFrame with the non-matching records.
        """
        # O 'id' e 'DT_BASE' são necessários, para garantir que não haverá repetição de valores
        # e que múltiplas transações numa mesma 'DT_BASE' não sejam excluídas
        df_captacao, df_cosmos_db = dfs
        df = df_captacao.join(df_cosmos_db, on=["id", "DT_BASE"], how="left_anti")
        df = df.select(
            fn.col("id").cast(StringType()),
            fn.col("DT_PARTITION").cast(StringType()),
            fn.col("ID_CLIENTE").cast(StringType()),
            fn.col("CD_CONTA").cast(StringType()),
            fn.col("CPF_CNPJ").cast(StringType()),
            fn.col("CLIENTE").cast(StringType()),
            fn.col("ID_ASSESSOR").cast(IntegerType()),
            fn.col("ASSESSOR").cast(StringType()),
            fn.col("FILIAL").cast(StringType()),
            fn.col("GRUPO").cast(StringType()),
            fn.col("COD_ASSESSOR_SINACOR").cast(IntegerType()),
            fn.col("VALOR").cast(DoubleType()),
            fn.col("TIPO").cast(StringType()),
            fn.col("FLUXO").cast(StringType()),
            fn.col("DT_BASE").cast(StringType()),
        )
        return df

    def load_cosmos_db(self, df: DataFrame) -> None:
        """
        Loads a DataFrame into a Delta table.

        Args:
            df (DataFrame): The DataFrame to load.
        """
        cfg = {
            "spark.cosmos.accountEndpoint": self.account_endpoint,
            "spark.cosmos.accountKey": self.account_key,
            "spark.cosmos.database": self.database,
            "spark.cosmos.container": self.container,
            "spark.cosmos.write.strategy": self.write_strategy,
            "spark.cosmos.write.bulk.enabled": True,
        }
        df.write.format("cosmos.oltp").options(**cfg).mode("append").save()

    def main(self) -> None:
        """
        Main function to execute the data processing pipeline.
        """
        logger.info("Starting ingestion process")

        logger.info("Establishing connecting to CosmosDB")
        client = self.instantiate_cosmos_client()
        database = self.get_database(client)
        container = self.get_container(database)

        logger.info(
            "Querying documents in CosmosDB between dates %s and %s",
            self.execution_date,
            self.current_date,
        )
        query = f"""
                SELECT c.id, c.DT_BASE FROM c
                WHERE c.DT_PARTITION >= '{self.execution_date}'
                AND c.DT_PARTITION <= '{self.current_date}'
            """
        items = self.query_items(query, container)

        logger.info("Reading DataFrame")
        df_captacao = self.read_captacao_table("captacao.captacao_geral")

        logger.info("Transforming DataFrame")
        df_captacao = self.create_primary_key_column(df_captacao)
        df_captacao = self.select_and_types_data(df_captacao)

        df = df_captacao
        logger.info("In this period there are %s records", df.count())
        if items:  # Se existem documentos no container
            logger.info("Filter records that do not exist documents in CosmosDB")
            df_cosmos_db = self.spark.createDataFrame(items)
            df = self.filter_non_matching_records([df, df_cosmos_db])
        if not df.rdd.isEmpty():
            logger.info("%s new records were found. Writing documents.", df.count())
            self.load_cosmos_db(df)
            logger.info("Ingestion process completed")
        else:
            logger.info("There are no documents to upload. Finishing job")


if __name__ == "__main__":
    args = sys.argv[1:]
    current_date = (datetime.now().date()).strftime("%Y-%m-%d")
    execution_date = args[0]
    config_cosmosdb = {
        "account_endpoint": args[1],
        "account_key": args[2],
        "database": args[3],
        "container": args[4],
        "write_strategy": args[5],
    }
    spark = SparkSession.builder.config(
        "spark.jars.packages",
        "com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.12.2",
    ).getOrCreate()

    etl = Captacao(spark, current_date, execution_date, config_cosmosdb)
    etl.main()
