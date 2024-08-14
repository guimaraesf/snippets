# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: cluster_config.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for configuring the Dataproc Cluster settings.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from dependencies.dados_alternativos.profissionais_liberais.variables import Variables
from google.cloud import dataproc_v1 as dataproc

# Here is the directory structure for running in Cloud Composer


class ClusterConfig(Variables):
    """
    This class is used to configure a cluster.
    """

    def __init__(
        self,
        pipeline_config: dict,
        dp_gce_cluster=dataproc.types.GceClusterConfig,
        dp_node_init_actions=dataproc.types.NodeInitializationAction,
        dp_instance_group_conf=dataproc.types.InstanceGroupConfig,
        dp_software_conf=dataproc.types.SoftwareConfig,
        dp_autoscaling_conf=dataproc.types.AutoscalingConfig,
        dp_endpoint_conf=dataproc.types.EndpointConfig,
        dp_cluster_conf=dataproc.types.ClusterConfig,
    ) -> None:
        """
        Initializes the ClusterConfig class.

        Args:
            pipeline_config (dict): The configuration for the pipeline.
        """
        self.pipeline_config = pipeline_config
        self.dp_gce_cluster = dp_gce_cluster
        self.dp_node_init_actions = dp_node_init_actions
        self.dp_instance_group_conf = dp_instance_group_conf
        self.dp_software_conf = dp_software_conf
        self.dp_autoscaling_conf = dp_autoscaling_conf
        self.dp_endpoint_conf = dp_endpoint_conf
        self.dp_cluster_conf = dp_cluster_conf
        super().__init__(self.pipeline_config)

    def get_labels(self) -> dict:
        """
        Retrieves the labels for the cluster.

        Returns:
            dict: A dictionary containing the labels for the cluster.
        """
        return {
            "application": str(self.get_label_application),
            "billing-id": str(self.get_label_application),
            "environment": str(self.get_label_environment),
            "cost-center": str(self.get_label_cost_center),
            "product": str(self.get_label_application),
            "resource": "google-dataproc-cluster",
            "squad": str(self.get_label_squad),
            "type": "dataproc-cluster",
            "value-stream": str(self.get_label_value_stream),
        }

    def get_gce_cluster_config(self) -> dataproc.types.GceClusterConfig:
        """
        Retrieves the Google Compute Engine (GCE) cluster configuration.

        Returns:
            dataproc.types.GceClusterConfig: The GCE cluster configuration.
        """
        subnetwork_name = (
            "https://www.googleapis.com/compute/v1/projects/{}/regions/{}/subnetworks/{}"
        )
        return self.dp_gce_cluster(
            metadata={"PIP_PACKAGES": self.get_pip_packages},
            zone_uri=self.get_zone,
            service_account=self.get_service_account,
            service_account_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            subnetwork_uri=subnetwork_name.format(
                self.get_network_id, self.get_region, self.get_subnetwork_id
            ),
            internal_ip_only=True,
        )

    def get_initialization_actions(
        self, cluster_name: str
    ) -> dataproc.types.NodeInitializationAction:
        """
        Retrieves the initialization actions for the cluster.

        Returns:
            dataproc.types.NodeInitializationAction: The initialization actions for the cluster.
        """
        # docker.sh is only needed for the CFFA process
        # As it is required for Chrome go installation.
        parts = self.get_cluster_id.split("-")
        parts.insert(-1, "cffa")
        cffa_cluster_name = "-".join(parts)
        executable_files = (
            ("docker.sh", "pip_install.sh")
            if cluster_name == cffa_cluster_name
            else ("pip_install.sh",)
        )
        return [
            self.dp_node_init_actions(
                executable_file=f"gs://{self.get_bucket_id}/scripts/config/{file}",
                execution_timeout="500s",
            )
            for file in executable_files
        ]

    def get_master_config(self) -> dataproc.types.InstanceGroupConfig:
        """
        Retrieves the master configuration for the cluster.

        Returns:
            dataproc.types.InstanceGroupConfig: The master configuration for the cluster.
        """
        return self.dp_instance_group_conf(
            num_instances=int(self.get_master_num_instances),
            machine_type_uri=self.get_machine_type_uri,
            disk_config={
                "boot_disk_type": self.get_disk_type,
                "boot_disk_size_gb": int(self.get_disk_size_gb),
                "num_local_ssds": 0,
                "local_ssd_interface": "SCSI",
            },
        )

    def get_worker_config(self) -> dataproc.types.InstanceGroupConfig:
        """
        Retrieves the worker configuration for the cluster.

        Returns:
            dataproc.types.InstanceGroupConfig: The worker configuration for the cluster.
        """
        return self.dp_instance_group_conf(
            num_instances=int(self.get_worker_num_instances),
            machine_type_uri=self.get_machine_type_uri,
            disk_config={
                "boot_disk_type": self.get_disk_type,
                "boot_disk_size_gb": int(self.get_disk_size_gb),
                "num_local_ssds": 0,
                "local_ssd_interface": "SCSI",
            },
        )

    def get_software_config(self) -> dataproc.types.SoftwareConfig:
        """
        Retrieves the software configuration for the cluster.

        Returns:
            dataproc.types.SoftwareConfig: The software configuration for the cluster.
        """
        return self.dp_software_conf(
            image_version=self.get_image_version,
            properties={
                "dataproc:dataproc.cluster.caching": "true",
                "dataproc:dataproc.allow.zero.workers": "true",
                "spark:spark.dataproc.enhanced.optimizer.enabled": "true",
                "spark:spark.dataproc.enhanced.execution.enabled": "true",
                "spark:spark.master": self.get_spark_master,
                "spark:spark.app.name": self.get_spark_app_name,
                "spark:spark.log.level": "INFO",
                "spark:spark.driver.log.layout": "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex",
                "spark:spark.eventLog.enabled": "true",
                "spark:spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark:spark.driver.memory": "2g",
                "spark:spark.driver.cores": "1",
                "spark:spark.driver.maxResultSize": "1g",
                "spark:spark.driver.memoryOverheadFactor": "0.10",
                "spark:spark.executor.cores": "2",
                "spark:spark.executor.memory": "2g",
                "spark:spark.executor.memoryOverhead": "600",
                "spark:spark.executor.memoryOverheadFactor": "0.10",
                "spark:spark.local.dir": self.get_tmp_dir,
                "spark:spark.logConf": "true",
                "spark:spark.decommission.enabled": "true",
                "spark:spark.yarn.executor.memoryOverhead": "600",
                "spark:spark.sql.debug.maxToStringFields": "100",
                "spark:spark.sql.files.maxPartitionBytes": "134217728",
                "spark:spark.sql.shuffle.partitions": "200",
                "spark:spark.sql.inMemoryColumnarStorage.compresse": "true",
                "yarn:yarn.resourcemanager.webapp.methods-allowed": "GET,POST,DELETE",
            },
        )

    def get_autoscaling_config(self) -> dataproc.types.AutoscalingConfig:
        """
        Retrieves the autoscaling configuration for the cluster.

        Returns:
            dataproc.types.AutoscalingConfig: The autoscaling configuration for the cluster.
        """
        policy_name = "projects/{}/regions/{}/autoscalingPolicies/{}"
        return self.dp_autoscaling_conf(
            policy_uri=policy_name.format(
                self.get_project_id, self.get_region, self.get_autoscaling_policy_id
            )
        )

    def get_endpoint_config(self) -> dataproc.types.EndpointConfig:
        """
        Retrieves the endpoint configuration for the cluster.

        Returns:
            dataproc.types.EndpointConfig: The endpoint configuration for the cluster.
        """
        return self.dp_endpoint_conf(enable_http_port_access=True)

    def get_cluster_config(self, cluster_name: str) -> dataproc.types.ClusterConfig:
        """
        Retrieves the cluster configuration.

        Returns:
            dataproc.types.ClusterConfig: The cluster configuration.
        """
        return self.dp_cluster_conf(
            config_bucket=self.get_bucket_id,
            gce_cluster_config=self.get_gce_cluster_config(),
            master_config=self.get_master_config(),
            worker_config=self.get_worker_config(),
            software_config=self.get_software_config(),
            autoscaling_config=self.get_autoscaling_config(),
            initialization_actions=self.get_initialization_actions(cluster_name),
            endpoint_config=self.get_endpoint_config(),
        )
