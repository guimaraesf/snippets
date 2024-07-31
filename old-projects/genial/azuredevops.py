import json
import logging
import sys

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import *
from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class AzureDevOps:
    def __init__(
        self,
        spark: SparkSession,
        execution_date: str,
        load_path: str,
        azure_devops: dict,
    ) -> None:
        self.spark = spark
        self.execution_date = execution_date
        self.load_path = load_path
        self.azure_devops = azure_devops
        self.remove_non_digits = r"[^\d]"
        self.remove_non_alphanumeric = r"[^a-zA-Z\d\s]"
        self.remove_extra_spaces = r"\s+"

    @property
    def __get_token(self) -> str:
        """
        Gets the personal access token for Azure DevOps.
        """
        return self.azure_devops["personal_access_token"]

    @property
    def __get_org_url(self) -> str:
        """
        Gets the organization url token for Azure DevOps.
        """
        return self.azure_devops["org_url"]

    @property
    def __get_list_of_projects(self) -> str:
        """
        Gets the list of projects for Azure DevOps.
        """
        return self.azure_devops["list_of_projects"]

    @property
    def __get_filter_date(self) -> str:
        """
        Gets the filter date for Azure DevOps.
        """
        return self.azure_devops["filter_date"]

    @staticmethod
    def _normalize_columns(df: DataFrame) -> DataFrame:
        """
        Replaces characters in a DataFrame column.

        Args:
            df (DataFrame): The DataFrame to be processed.

        Returns:
            DataFrame: The processed DataFrame.
        """
        CHARS_WITH_ACCENTS = "áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇäëïöüÄËÏÖÜ"
        CHARS_WITHOUT_ACCENTS = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaoAOcCaeiouAEIOU"
        for column in df.columns:
            if not column.endswith("SK"):
                df = df.withColumn(
                    column,
                    fn.trim(
                        fn.upper(
                            fn.translate(
                                column,
                                CHARS_WITH_ACCENTS,
                                CHARS_WITHOUT_ACCENTS,
                            )
                        )
                    ),
                )
        return df

    def _apply_regexpr_and_data_type(
        self,
        df: DataFrame, col_infos: dict
    ) -> DataFrame:
        """
        Applies a regular expression replacement to a DataFrame column.

        Args:
            df (DataFrame): The DataFrame to be processed.
            col_infos (dict): The dictionary with infos of columns.

        Returns:
            DataFrame: The processed DataFrame.
        """
        for col, (regex_expr, data_type) in col_infos.items():
            pattern, replacement = regex_expr
            df = df.withColumn(
                col, 
                fn.regexp_replace(
                    fn.regexp_replace(col, pattern, replacement).cast(data_type), 
                    self.remove_extra_spaces, 
                    r" "
                )
            )
        df = df.select(list(col_infos.keys()))
        return df

    def _get_response(self, url: str) -> list:
        """
        Get the response from the request.

        Returns:
            list: The response from the request.

        Raises:
            HTTPError: If the status of the response is not 200.
        """
        HEADERS = {"Content-Type": "application/json"}
        with requests.Session() as session:
            response = session.get(
                url, headers=HEADERS, auth=HTTPBasicAuth("", self.__get_token)
            )
            if response.status_code == 200:
                return response.json()
            raise requests.exceptions.HTTPError(
                f"HTTP Error {response.status_code}: {response.reason}"
            )

    def read_and_transform_users_table(self) -> DataFrame:
        """
        This function reads and transforms the Users table.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        def read_from_request() -> DataFrame:
            url = f"{self.__get_org_url}/_odata/v4.0-preview/Users"
            response = self._get_response(url)
            df = self.spark.createDataFrame(pd.DataFrame(response["value"]))
            return df

        def clean_and_type_columns(df: DataFrame) -> DataFrame:
            col_infos = {
                "UserSK": ((r"", r""), StringType()),
                "UserName": ((self.remove_non_alphanumeric, r" "), StringType()),
            }
            df = self._apply_regexpr_and_data_type(df, col_infos)
            return df

        df = read_from_request()
        df = self._normalize_columns(df)
        df = clean_and_type_columns(df)
        return df

    def read_and_transform_work_items_table(self) -> DataFrame:
        """
        This function reads and transforms the Work Items table.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        def read_from_request() -> DataFrame:
            dfs = []
            range_date = f"ChangedDate ge {self.__get_filter_date}"
            for project_id in self.__get_list_of_projects:
                url = f"{self.__get_org_url}/{project_id}/_odata/v4.0-preview/WorkItems?$filter={range_date}"
                response = self._get_response(url)
                df = pd.DataFrame(response["value"])
                dfs.append(df)
            df = self.spark.createDataFrame(pd.concat(dfs))
            df = df.filter(
                fn.date_format(
                    fn.to_timestamp(fn.col("ChangedDate")), "yyyy-MM-dd"
                )
                <= self.execution_date
            )
            return df

        def format_date_columns(df: DataFrame) -> DataFrame:
            date_columns = (
                "InProgressDate",
                "CompletedDate",
                "AnalyticsUpdatedDate",
                "ChangedDate",
                "CreatedDate",
                "ActivatedDate",
                "ClosedDate",
                "ResolvedDate",
                "FinishDate",
                "StartDate",
                "TargetDate",
                "StateChangeDate",
            )
            for col in date_columns:
                df = df.withColumn(
                    col,
                    fn.date_format(
                        fn.from_utc_timestamp(
                            fn.col(col), "America/Sao_Paulo"
                        ),
                        "yyyy-MM-dd HH:mm:ss.SSSXXX",
                    ),
                )
            return df

        def clean_and_type_columns(df: DataFrame) -> DataFrame:
            col_infos = {
                "WorkItemId": ((self.remove_non_digits, r""), IntegerType()),
                "InProgressDate": ((r"", r""), StringType()),
                "CompletedDate": ((r"", r""), StringType()),
                "LeadTimeDays": ((r"[^\d.]", r""), DoubleType()),
                "CycleTimeDays": ((r"[^\d.]", r""), DoubleType()),
                "AnalyticsUpdatedDate": ((r"", r""), StringType()),
                "ProjectSK": ((r"", r""), StringType()),
                "AreaSK": ((r"", r""), StringType()),
                "IterationSK": ((r"", r""), StringType()),
                "AssignedToUserSK": ((r"", r""), StringType()),
                "ChangedByUserSK": ((r"", r""), StringType()),
                "CreatedByUserSK": ((r"", r""), StringType()),
                "ActivatedByUserSK": ((r"", r""), StringType()),
                "ClosedByUserSK": ((r"", r""), StringType()),
                "Title": ((self.remove_non_alphanumeric, r""), StringType()),
                "WorkItemType": ((self.remove_non_alphanumeric, r" "), StringType()),
                "ChangedDate": ((r"", r""), StringType()),
                "CreatedDate": ((r"", r""), StringType()),
                "State": ((self.remove_non_alphanumeric, r""), StringType()),
                "ActivatedDate": ((r"", r""), StringType()),
                "ClosedDate": ((r"", r""), StringType()),
                "ResolvedDate": ((r"", r""), StringType()),
                "FinishDate": ((r"", r""), StringType()),
                "StartDate": ((r"", r""), StringType()),
                "TargetDate": ((r"", r""), StringType()),
                "ParentWorkItemId": ((self.remove_non_digits, r""), IntegerType()),
                "TagNames": ((self.remove_non_alphanumeric, r""), StringType()),
                "StateCategory": ((self.remove_non_alphanumeric, r""), StringType()),
                "StateChangeDate": ((r"", r""), StringType()),
                "Custom_Custoestimado": ((r"[^\d.]", r""), DoubleType()),
                "Custom_Custorealizado": ((r"[^\d.]", r""), DoubleType()),
                "Custom_Projeto": ((self.remove_non_alphanumeric, r""), StringType()),
                "Custom_Tipodeprojeto": ((self.remove_non_alphanumeric, r""), StringType()),
            }
            df = self._apply_regexpr_and_data_type(df, col_infos)
            return df

        df = read_from_request()
        df = format_date_columns(df)
        df = self._normalize_columns(df)
        df = clean_and_type_columns(df)
        return df

    def read_and_transform_iterations_table(self) -> DataFrame:
        """
        This function reads and transforms the Iterations table.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        def read_from_request() -> DataFrame:
            url = f"{self.__get_org_url}/_odata/v4.0-preview/Iterations"
            response = self._get_response(url)
            df = self.spark.createDataFrame(pd.DataFrame(response["value"]))
            return df

        def format_date_columns(df: DataFrame) -> DataFrame:
            date_columns = {
                "StartDate": "IterationStartDate",
                "EndDate": "IterationEndDate",
            }
            for col_name, new_col_name in date_columns.items():
                df = df.withColumn(
                    col_name, fn.substring(fn.col(col_name), 0, 10)
                )
                df = df.withColumnRenamed(col_name, new_col_name)
            return df

        def clean_and_type_columns(df: DataFrame) -> DataFrame:
            col_infos = {
                "IterationSK": ((r"", r""), StringType()),
                "IterationName": (
                    (self.remove_non_alphanumeric, r" "),
                    StringType(),
                ),
                "IterationStartDate": ((r"", r""), StringType()),
                "IterationEndDate": ((r"", r""), StringType()),
            }
            df = self._apply_regexpr_and_data_type(df, col_infos)
            return df

        df = read_from_request()
        df = format_date_columns(df)
        df = self._normalize_columns(df)
        df = clean_and_type_columns(df)
        return df

    def read_and_transform_areas_table(self) -> DataFrame:
        """
        This function reads and transforms the Areas table.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        def read_from_request() -> DataFrame:
            url = f"{self.__get_org_url}/_odata/v4.0-preview/Areas"
            response = self._get_response(url)
            df = self.spark.createDataFrame(pd.DataFrame(response["value"]))
            return df

        def clean_and_type_columns(df: DataFrame) -> DataFrame:
            col_infos = {
                "AreaSK": ((r"", r""), StringType()),
                "AreaName": ((self.remove_non_alphanumeric, r" "), StringType()),
            }
            df = self._apply_regexpr_and_data_type(df, col_infos)
            return df

        df = read_from_request()
        df = self._normalize_columns(df)
        df = clean_and_type_columns(df)
        return df

    def read_and_transform_projects_table(self) -> DataFrame:
        """
        This function reads and transforms the Projects table.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        def read_from_request() -> DataFrame:
            url = f"{self.__get_org_url}/_odata/v4.0-preview/Projects"
            response = self._get_response(url)
            df = self.spark.createDataFrame(pd.DataFrame(response["value"]))
            return df

        def clean_and_type_columns(df: DataFrame) -> DataFrame:
            col_infos = {
                "ProjectSK": ((r"", r""), StringType()),
                "ProjectName": ((self.remove_non_alphanumeric, r" "), StringType()),
            }
            df = self._apply_regexpr_and_data_type(df, col_infos)
            df = df.select(list(col_infos.keys()))
            return df

        df = read_from_request()
        df = self._normalize_columns(df)
        df = clean_and_type_columns(df)
        return df

    def transform_df(self, dfs: list[DataFrame]) -> DataFrame:
        """
        This function transforms a list of DataFrames.

        Args:
            dfs (list[DataFrame]): A list of DataFrames to be transformed.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        df_work_items, df_area, df_iterations, df_users, df_projects = dfs
        df = df_work_items.join(df_projects, on=["ProjectSK"], how="left")
        df = df.join(df_area, on=["AreaSK"], how="left")
        df = df.join(df_iterations, on=["IterationSK"], how="left")

        columns = {
            "AssignedToUserSK": (
                "AssignedToUserName",
                ["AssignedToUserSK", "UserSK"],
            ),
            "ChangedByUserSK": (
                "ChangedByUserName",
                ["ChangedByUserSK", "UserSK"],
            ),
            "CreatedByUserSK": (
                "CreatedByUserName",
                ["CreatedByUserSK", "UserSK"],
            ),
            "ActivatedByUserSK": (
                "ActivatedByUserName",
                ["ActivatedByUserSK", "UserSK"],
            ),
            "ClosedByUserSK": (
                "ClosedByUserName",
                ["ClosedByUserSK", "UserSK"],
            ),
        }

        for col_name, (new_col_name, cols_to_drop) in columns.items():
            df = df.join(
                df_users, on=[df[col_name] == df_users["UserSK"]], how="left"
            ).drop(*cols_to_drop)
            df = df.withColumnRenamed("UserName", new_col_name)

        return df

    def select_and_data_type(self, df: DataFrame) -> DataFrame:
        """
        This function selects and types the columns of a DataFrame.

        Args:
            df (DataFrame): The DataFrame to be selected and typed.

        Returns:
            DataFrame: The selected and typed DataFrame.
        """
        return df.select(
            fn.col("WorkItemId").cast(IntegerType()).alias("WorkItemId"),
            fn.col("InProgressDate").cast(TimestampType()).alias("InProgressDate"),
            fn.col("CompletedDate").cast(TimestampType()).alias("CompletedDate"),
            fn.col("LeadTimeDays").cast(IntegerType()).alias("LeadTimeDays"),
            fn.col("CycleTimeDays").cast(IntegerType()).alias("CycleTimeDays"),
            fn.col("AnalyticsUpdatedDate").cast(TimestampType()).alias("AnalyticsUpdatedDate"),
            fn.col("Title").cast(StringType()).alias("Title"),
            fn.col("WorkItemType").cast(StringType()).alias("WorkItemType"),
            fn.col("ChangedDate").cast(TimestampType()).alias("ChangedDate"),
            fn.col("CreatedDate").cast(TimestampType()).alias("CreatedDate"),
            fn.col("State").cast(StringType()).alias("State"),
            fn.col("ActivatedDate").cast(TimestampType()).alias("ActivatedDate"),
            fn.col("ClosedDate").cast(TimestampType()).alias("ClosedDate"),
            fn.col("ResolvedDate").cast(TimestampType()).alias("ResolvedDate"),
            fn.col("FinishDate").cast(TimestampType()).alias("FinishDate"),
            fn.col("StartDate").cast(TimestampType()).alias("StartDate"),
            fn.col("TargetDate").cast(TimestampType()).alias("TargetDate"),
            fn.col("ParentWorkItemId").cast(IntegerType()).alias("ParentWorkItemId"),
            fn.col("TagNames").cast(StringType()).alias("TagNames"),
            fn.col("StateCategory").cast(StringType()).alias("StateCategory"),
            fn.col("StateChangeDate").cast(TimestampType()).alias("StateChangeDate"),
            fn.round(fn.col("Custom_Custoestimado").cast(DoubleType()), 2).alias("CustoEstimado"),
            fn.round(fn.col("Custom_Custorealizado").cast(DoubleType()), 2).alias("CustoRealizado"),
            fn.col("Custom_Projeto").cast(StringType()).alias("Projeto"),
            fn.col("Custom_Tipodeprojeto").cast(StringType()).alias("TipoProjeto"),
            fn.col("ProjectSK").cast(StringType()).alias("ProjectSK"),
            fn.col("ProjectName").cast(StringType()).alias("ProjectName"),
            fn.col("AreaSK").cast(StringType()).alias("AreaSK"),
            fn.col("AreaName").cast(StringType()).alias("AreaName"),
            fn.col("IterationSK").cast(StringType()).alias("IterationSK"),
            fn.col("IterationName").cast(StringType()).alias("IterationName"),
            fn.col("IterationStartDate").cast(DateType()).alias("IterationStartDate"),
            fn.col("IterationEndDate").cast(DateType()).alias("IterationEndDate"),
            fn.col("AssignedToUserName").cast(StringType()).alias("AssignedToUserName"),
            fn.col("ChangedByUserName").cast(StringType()).alias("ChangedByUserName"),
            fn.col("CreatedByUserName").cast(StringType()).alias("CreatedByUserName"),
            fn.col("ActivatedByUserName").cast(StringType()).alias("ActivatedByUserName"),
            fn.col("ClosedByUserName").cast(StringType()).alias("ClosedByUserName"),
        )

    def load(self, df: DataFrame) -> None:
        """
        Loads a DataFrame into a Delta table.

        Args:
            df (DataFrame): The DataFrame to load.
        """
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("replaceWhere", f"DT_PARTITION = '{self.execution_date}'")
            .partitionBy("DT_PARTITION")
            .save(self.load_path)
        )

    def main(self) -> None:
        """
        Main function to execute the data processing pipeline.
        """
        logger.info("Starting ingestion process")

        logger.info("Getting DataFrames")
        df_work_items = self.read_and_transform_work_items_table()
        df_projects = self.read_and_transform_projects_table()
        df_area = self.read_and_transform_areas_table()
        df_iterations = self.read_and_transform_iterations_table()
        df_users = self.read_and_transform_users_table()

        logger.info("Transforming DataFrames")
        dfs = [df_work_items, df_area, df_iterations, df_users, df_projects]
        df = self.transform_df(dfs)

        if not df.rdd.isEmpty():
            logger.info(
                "%s work items were found for date %s",
                df.count(),
                self.execution_date,
            )
            df = self.select_and_data_type(df)
            logger.info("Loading DataFrame in table")
            df = df.withColumn("DT_PARTITION", fn.lit(self.execution_date))
            self.load(df)
            logger.info("The data has been successfully loaded into the table")
        else:
            logger.info("Finishing job")


if __name__ == "__main__":
    args = sys.argv[1:]
    
    EXECUTION_DATE = args[0]
    LOAD_PATH = args[1]
    AZURE_DEVOPS_PARAMS = json.loads(args[2])

    spark = SparkSession.builder.getOrCreate()
    etl = AzureDevOps(
        spark,
        EXECUTION_DATE,
        LOAD_PATH,
        AZURE_DEVOPS_PARAMS,
    )
    etl.main()
