import asyncio
import json
import logging
import sys

import aiohttp
import nest_asyncio
import pyspark.sql.functions as fn
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, TimestampType


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class MktCloudCartoes:
    """
    This class handles a data processing pipeline for Salesforce.

    It retrieves and transforms data from various tables, creates payloads from the transformed data,
    sends POST requests with the payloads, and loads the transformed data into a Delta table.
    """

    def __init__(
        self,
        spark: SparkSession,
        date: str,
        load_path: str,
        api_credentials: dict,
    ) -> None:
        self.spark = spark
        self.date = date
        self.load_path = load_path
        self.api_credentials = api_credentials

    def get_person_table(self, table_name: str) -> DataFrame:
        """
        Retrieves a person table from the Spark session.

        Args:
            table_name (str): The name of the table to retrieve.

        Returns:
            DataFrame: The retrieved table.
        """
        # Remove acentos
        chars_with_accents = "áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇäëïöüÄËÏÖÜ"
        chars_without_accents = (
            "aeiouAEIOUaeiouAEIOUaeiouAEIOUaoAOcCaeiouAEIOU"
        )
        df = self.spark.table(table_name).select(
            fn.col("person_id").alias("person_id"),
            fn.col("person_type").alias("Persontype"),
            fn.translate(
                fn.col("name"), chars_with_accents, chars_without_accents
            ).alias("Fullname"),
            fn.col("tax_identification_number").alias("Cpf"),
        )
        return df

    def get_account_table(self, table_name: str) -> DataFrame:
        """
        Retrieves an account table from the Spark session.

        Args:
            table_name (str): The name of the table to retrieve.

        Returns:
            DataFrame: The retrieved table.
        """
        df = self.spark.table(table_name).select(
            fn.col("person_id").alias("person_id"),
            fn.col("account_id").alias("Accountid"),
            fn.col("create_date").alias("Createdate"),
            fn.col("due_day").alias("Dueday"),
            fn.col("status_date").alias("Statusdate"),
            fn.col("next_due_date").alias("Nextduedate"),
            fn.col("next_real_due_date").alias("Nextrealduedate"),
            fn.col("product_id").alias("Productid"),
            fn.col("product_description").alias("Productdescription"),
        )
        return df

    def get_card_table(self, table_name: str) -> DataFrame:
        """
        Retrieves a card table from the Spark session.

        Args:
            table_name (str): The name of the table to retrieve.

        Returns:
            DataFrame: The retrieved table.
        """
        df = self.spark.table(table_name).select(
            fn.col("person_id").alias("person_id"),
            fn.col("account_id").alias("Accountid"),
            fn.col("card_id").alias("CardId"),
            fn.regexp_replace(fn.col("pan"), "[*]", "").alias("Pan"),
            fn.col("bin").alias("Bin"),
            fn.col("name_on_card").alias("Nameoncard"),
            fn.col("owner").alias("Owner"),
            fn.col("is_temporary_card").alias("Istemporarycard"),
            fn.col("status_id").alias("Statusid"),
            fn.col("status_description").alias("Statusdescription"),
            fn.col("status_allow_approve").alias("Statusallowapprove"),
            fn.col("expiration_date").alias("Expirationdate"),
        )
        return df

    def transform_df(self, dfs: list[DataFrame]) -> DataFrame:
        """
        Transforms a list of DataFrames into a single DataFrame.

        Args:
            dfs (list[DataFrame]): The list of DataFrames to transform.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        df_person, df_account, df_card = dfs
        df_account = df_account.filter(
            fn.to_date(fn.col("create_date")) == self.date
        )
        df = df_person.join(df_account, ["person_id"], "inner")
        df = df.join(df_card, ["person_id", "Accountid"], "inner")
        df = df.select(
            fn.col("Persontype").cast(StringType()),
            fn.col("Fullname").cast(StringType()),
            fn.col("Cpf").cast(StringType()),
            fn.col("Accountid").cast(StringType()),
            fn.col("Createdate").cast(StringType()),
            fn.col("Dueday").cast(StringType()),
            fn.col("Statusdate").cast(TimestampType()),
            fn.col("Nextduedate").cast(TimestampType()),
            fn.col("Nextrealduedate").cast(TimestampType()),
            fn.col("Productid").cast(StringType()),
            fn.col("Productdescription").cast(StringType()),
            fn.col("CardId").cast(StringType()),
            fn.col("Pan").cast(StringType()),
            fn.col("Bin").cast(StringType()),
            fn.col("Nameoncard").cast(StringType()),
            fn.col("Owner").cast(StringType()),
            fn.col("Istemporarycard").cast(StringType()),
            fn.col("Statusid").cast(StringType()),
            fn.col("Statusdescription").cast(StringType()),
            fn.col("Statusallowapprove").cast(StringType()),
            fn.col("Expirationdate").cast(TimestampType()),
        )
        return df

    @staticmethod
    def create_payloads_from_df(df: DataFrame) -> list:
        """
        Creates payloads from a DataFrame.

        Args:
            df (DataFrame): The DataFrame to create payloads from.

        Returns:
            list: The created payloads.
        """
        df = df.withColumn("keys", fn.struct("CardId"))
        df = df.withColumn(
            "values",
            fn.struct(
                "CardId",
                "Persontype",
                "Fullname",
                "Cpf",
                "Accountid",
                "Createdate",
                "Dueday",
                "Statusdate",
                "Nextduedate",
                "Nextrealduedate",
                "Productid",
                "Productdescription",
                "Pan",
                "Bin",
                "Nameoncard",
                "Owner",
                "Istemporarycard",
                "Statusid",
                "Statusdescription",
                "Statusallowapprove",
                "Expirationdate",
            ),
        )
        df = df.select("keys", "values")
        df = df.withColumn("payload", fn.to_json(fn.struct("keys", "values")))
        payloads = df.select("payload").collect()
        return payloads

    def send_post_request_token(self) -> str:
        """
        Sends a POST request to retrieve a token.

        Returns:
            str: The retrieved token.
        """
        URL = self.api_credentials["url_token"]
        BODY = self.api_credentials["body"]
        HEADERS = {"Content-Type": "application/json"}
        with requests.Session() as session:
            response = session.post(
                url=URL, headers=HEADERS, json=BODY, timeout=60
            )
            if response.status_code == 200:
                token = response.json()
                return token["access_token"]
            raise requests.exceptions.HTTPError(
                f"HTTP Error {response.status_code}: {response.reason}"
            )

    async def send_post_request(self, payload: list, token: str) -> bool:
        """
        Sends a POST request with a payload and a token.

        Args:
            payload (list): The payload to send.
            token (str): The token to use.

        Returns:
            bool: True if the request was successful, False otherwise.
        """
        URL = self.api_credentials["url_rest"]
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        # Número máximo de requisições executadas simultaneamente
        async with asyncio.Semaphore(1000):
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    URL, headers=headers, json=payload, timeout=3600
                )
                await asyncio.sleep(1)
                if response.status == 200:
                    return True
                return False

    async def send_post_request_payloads(
        self, payloads: list, token: str
    ) -> None:
        """
        Sends POST requests with payloads and a token.

        Args:
            payloads (list): The payloads to send.
            token (str): The token to use.
        """
        tasks = []
        count = 0
        try:
            for row in payloads:
                payload = [json.loads(row["payload"])]
                post_request = asyncio.ensure_future(
                    self.send_post_request(payload, token)
                )
                tasks.append(post_request)
                count += 1
                # A cada 1000 requisições, espera-se 60 segundos para um novo lote de envios
                if count % 1000 == 0:
                    await asyncio.sleep(60)
            responses = await asyncio.gather(*tasks)
            if all(responses):
                logger.info(
                    "All %s payloads have been posted successfully",
                    len(responses),
                )
        except aiohttp.ClientResponseError as e:
            logger.warning("Only %s payloads were send", count)
            raise aiohttp.ClientResponseError(e.request_info, e.history)

    def load(self, df: DataFrame) -> None:
        """
        Loads a DataFrame into a Delta table.

        Args:
            df (DataFrame): The DataFrame to load.
        """
        (
            df.write.format("delta")
            .mode("append")
            .option("overwriteSchema", "true")
            .partitionBy("DT_PARTITION")
            .save(self.load_path)
        )

    async def main(self):
        """
        Main function to execute the data processing pipeline.
        """
        logger.info("Starting ingestion process")
        logger.info("Getting DataFrames")
        df_person = self.get_person_table("datahub.person")
        df_account = self.get_account_table("datahub.account")
        df_card = self.get_card_table("datahub.card")

        logger.info("Transforming DataFrames")
        df = self.transform_df([df_person, df_account, df_card])
        logger.info("%s new records were found", df.count())

        if not df.rdd.isEmpty():
            logger.info("Creating payload for to send to Salesforce")
            payloads = self.create_payloads_from_df(df)

            logger.info("Getting access token")
            token = self.send_post_request_token()
            logger.info("Sending data to Salesforce")
            await self.send_post_request_payloads(payloads, token)

            logger.info("Loading DataFrame in table")
            df = df.withColumn("DT_PARTITION", fn.lit(self.date))
            self.load(df)
            logger.info("The data has been successfully loaded into the table")
            logger.info("Ingestion process completed")
        else:
            logger.info("Finishing job")

if __name__ == "__main__":
    args = sys.argv[1:]
    EXECUTION_DATE = args[0]
    LOAD_PATH = args[1]
    MKT_CLOUD_API_CREDENTIALS = json.loads(args[2])

    nest_asyncio.apply()
    spark = SparkSession.builder.getOrCreate()
    etl = MktCloudCartoes(
        spark, EXECUTION_DATE, LOAD_PATH, MKT_CLOUD_API_CREDENTIALS
    )
    asyncio.run(etl.main())
