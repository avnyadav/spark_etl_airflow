import logging
import shutil
from datetime import datetime

from airflow.models.xcom_arg import XComArg
from airflow.decorators import dag, task

log = logging.getLogger(__name__)
import sys, os

PACKAGE_PATH = os.path.join("dags", "dist")
package_names = os.listdir(PACKAGE_PATH)
if len(package_names) == 0:
    raise Exception("Custom package not found")
else:
    package_names = os.path.join(PACKAGE_PATH, package_names[0])

if not shutil.which("virtualenv"):
    log.warning("The tutorial_taskflow_api_etl_virtualenv example DAG requires virtualenv, please install it.")
else:
    @dag(schedule_interval=None, start_date=datetime.now(), catchup=False, tags=['finance_complaint'])
    def finance_complaint_etl():
        @task(multiple_outputs=True)
        def initialization():
            from etl_project.finance_complaint_constant import constant
            from etl_project.finance_complaint_config.config import FinanceComplaintPipelineConfig
            finance_complaint_config = FinanceComplaintPipelineConfig()
            config = {constant.CONFIG_KEY_NAME:
                {
                    constant.PIPELINE_CONFIG_KEY_NAME: finance_complaint_config.get_pipeline_config(),
                    constant.EXTRACT_CONFIG_KEY_NAME: finance_complaint_config.get_extraction_config(),
                    constant.TRANSFORM_CONFIG_KEY_NAME: finance_complaint_config.get_transformation_config(),
                    constant.LOAD_CONFIG_KEY_NAME: finance_complaint_config.get_loading_config()
                }
            }
            return config

        @task.virtualenv(
            use_dill=True,
            requirements=[package_names])
        def extract(config: dict):
            from etl_project.finance_complaint_constant import constant
            from etl_project.finance_complaint_config.config import ExtractConfig
            from etl_project.finance_complaint_etl.extract import Extract
            extract_config = config[constant.CONFIG_KEY_NAME][constant.EXTRACT_CONFIG_KEY_NAME]
            extract_config = ExtractConfig(*(extract_config))
            extractor = Extract(extract_config=extract_config)
            extract_output = extractor.start_extraction()

            config.update({
                constant.OUTPUT_KEY_NAME:
                    {
                        constant.EXTRACT_OUTPUT_KEY: extract_output}
            })
            return config

        @task.virtualenv(
            use_dill=True,
            requirements=[package_names, "pyspark==3.2.1", "findspark"])
        def transform(config):
            from pyspark.sql import SparkSession
            from etl_project.finance_complaint_config.config import TransformConfig
            from etl_project.finance_complaint_constant import constant
            from etl_project.finance_complaint_etl.extract import ExtractOutput
            from etl_project.finance_complaint_etl.transform import Transform
            extract_output = config[constant.OUTPUT_KEY_NAME][constant.EXTRACT_OUTPUT_KEY]
            transform_config = config[constant.CONFIG_KEY_NAME][constant.TRANSFORM_CONFIG_KEY_NAME]
            import json
            import os
            data = json.dumps(config)
            import subprocess
            # subprocess.Popen(["pip","install","pyspark==3.2.1"])
            # subprocess.Popen(["pip", "install", "findspark"])
            # subprocess.Popen(["spark-submit", "dags/etl_project/finance_complaint_etl/transform.py", data])
            extract_output = ExtractOutput(*(extract_output))
            transform_config = TransformConfig(*(transform_config))

            transformer = Transform(extract_output=extract_output,
                                    transform_config=transform_config)
            transform_output = transformer.start_transformation()
            config.update({
                constant.OUTPUT_KEY_NAME: {
                    constant.TRANSFORM_OUTPUT_KEY: transform_output
                }
            })
            #config = json.load(open(os.path.join("data", "report.json"), "r"))
            return config

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=[package_names])
        def load(config):
            from etl_project.finance_complaint_config.config import PipelineConfig, LoadConfig

            from etl_project.finance_complaint_constant import constant
            from etl_project.finance_complaint_etl.load import Load, TransformOutput
            pipeline_config = config[constant.CONFIG_KEY_NAME][constant.PIPELINE_CONFIG_KEY_NAME]
            transform_output = config[constant.OUTPUT_KEY_NAME][constant.TRANSFORM_OUTPUT_KEY]
            load_config = config[constant.CONFIG_KEY_NAME][constant.LOAD_CONFIG_KEY_NAME]

            transform_output = TransformOutput(*(transform_output))
            load_config = LoadConfig(*(load_config))
            pipeline_config = PipelineConfig(*(pipeline_config))

            loader = Load(load_config=load_config, transform_output=transform_output,
                          pipeline_config=pipeline_config)

            load_output = loader.start_loading()
            print(load_output)

        init_config = initialization()
        extract_config_n_output = extract(config=init_config)
        transform_config_n_output = transform(config=extract_config_n_output)
        load(config=transform_config_n_output)


    finance_complaint_etl_pipeline = finance_complaint_etl()
