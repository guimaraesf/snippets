# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: variables.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module contains several properties that return various attributes.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


class Variables:
    """
    It contains several properties that return various attributes.
    """

    def __init__(self, pipeline_config: dict) -> None:
        """
        Initializes the Variables class.

        Args:
            pipeline_config (dict): The configuration for the pipeline.
        """
        self.pipeline_config = pipeline_config
        self.set_attributes()

    def set_attributes(self) -> None:
        """
        Sets the attributes of the class instance based on the keys and values.
        """
        for name, value in self.pipeline_config.items():
            setattr(self, name, value)

    @property
    def get_autoscaling_policy_id(self):
        """Returns the ID of the autoscaling policy."""
        return self.AUTOSCALING_POLICY_ID

    @property
    def get_bucket_id(self):
        """Returns the ID of the bucket."""
        return self.BUCKET_ID

    @property
    def get_cluster_id(self):
        """Returns the ID of the cluster."""
        return self.CLUSTER_ID

    @property
    def get_dataset_id(self):
        """Returns the ID of the dataset."""
        return self.DATASET_ID

    @property
    def get_disk_size_gb(self):
        """Returns the disk size in gigabytes."""
        return self.DISK_SIZE_GB

    @property
    def get_disk_type(self):
        """Returns the type of the disk."""
        return self.DISK_TYPE

    @property
    def get_email(self):
        """Returns the email."""
        return self.EMAIL

    @property
    def get_image_version(self):
        """Returns the version of the image."""
        return self.IMAGE_VERSION

    @property
    def get_label_application(self):
        """Returns the application label."""
        return self.LABEL_APPLICATION

    @property
    def get_label_cost_center(self):
        """Returns the cost center label."""
        return self.LABEL_COST_CENTER

    @property
    def get_label_environment(self):
        """Returns the environment label."""
        return self.LABEL_ENVIRONMENT

    @property
    def get_label_squad(self):
        """Returns the squad label."""
        return self.LABEL_SQUAD

    @property
    def get_label_value_stream(self):
        """Returns the value stream label."""
        return self.LABEL_VALUE_STREAM

    @property
    def get_machine_type_uri(self):
        """Returns the URI of the machine type."""
        return self.MACHINE_TYPE_URI

    @property
    def get_master_num_instances(self):
        """Returns the number of master instances."""
        return self.MASTER_NUM_INSTANCES

    @property
    def get_network_id(self):
        """Returns the ID of the network."""
        return self.NETWORK_ID

    @property
    def get_pip_packages(self):
        """Returns the pip packages."""
        return self.PIP_PACKAGES

    @property
    def get_port(self):
        """Returns the port."""
        return self.PORT

    @property
    def get_project_id(self):
        """Returns the ID of the project."""
        return self.PROJECT_ID

    @property
    def get_region(self):
        """Returns the region."""
        return self.REGION

    @property
    def get_service_account(self):
        """Returns the service account."""
        return self.SERVICE_ACCOUNT

    @property
    def get_spark_app_name(self):
        """Returns the name of the Spark application."""
        return self.SPARK_APP_NAME

    @property
    def get_spark_master(self):
        """Returns the Spark master."""
        return self.SPARK_MASTER

    @property
    def get_subnetwork_id(self):
        """Returns the ID of the subnetwork."""
        return self.SUBNETWORK_ID

    @property
    def get_tmp_dir(self):
        """Returns the temporary directory."""
        return self.TMP_DIR

    @property
    def get_worker_num_instances(self):
        """Returns the number of worker instances."""
        return self.WORKER_NUM_INSTANCES

    @property
    def get_zone(self):
        """Returns the zone."""
        return self.ZONE
