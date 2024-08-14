# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: strategy.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description:
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from __future__ import annotations

from abc import ABC
from abc import abstractmethod

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class ITransformer(ABC):
    @abstractmethod
    def get_schema(self) -> StructType:
        pass

    @abstractmethod
    def init_sparksession(
        self, master: str, app_name: str, bucket_id: str, dataset_id: str, project_id: str
    ) -> SparkSession:
        pass

    @abstractmethod
    def get_bucket_obj(self, project_id: str, bucket_id: str) -> storage.Bucket:
        pass

    @abstractmethod
    def get_list_blobs(self, bucket_obj: storage.Bucket, dir_name: str) -> list[str]:
        pass

    @abstractmethod
    def df_is_valid(self, df) -> bool:
        pass

    @abstractmethod
    def build_df_schema(self, table_infos: dict) -> None:
        pass

    @abstractmethod
    def process_blob(
        self,
        spark: SparkSession,
        bucket_id: str,
        blob_name: str,
        table_infos: dict,
        df_params: tuple,
    ) -> None:
        pass

    @abstractmethod
    def process_all_blobs(
        self,
        spark: SparkSession,
        bucket_id: str,
        blob_names: list,
        table_infos: dict,
        df_params: tuple,
    ) -> None:
        pass

    @abstractmethod
    def check_before_processing(self, blob_names: list) -> bool:
        pass

    @abstractmethod
    def run_transform(self) -> None:
        pass
