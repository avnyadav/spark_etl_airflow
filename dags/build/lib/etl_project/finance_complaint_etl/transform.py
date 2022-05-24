from etl_project.finance_complaint_etl.extract import ExtractOutput
from etl_project.finance_complaint_config.config import TransformConfig
from collections import namedtuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import logging
import findspark
from etl_project.finance_complaint_constant import constant

findspark.init()
TransformOutput = namedtuple("TransformOutput", ["transform_dir"])


class Transform:
    def __init__(self, extract_output: ExtractOutput, transform_config: TransformConfig) -> None:
        try:
            logging.info(f"Starting Transformation..")
            self.extract_output = extract_output
            self.transform_config = transform_config

        except Exception as e:
            raise e

    def start_transformation(self) -> TransformOutput:
        try:
            dir_name = self.extract_output.extract_dir

            spark_session = SparkSession.builder.master("local[*]").appName("FinanceComplaint").getOrCreate()

            # Transforming each file 
            for file_name in os.listdir(dir_name):
                file_path = os.path.join(dir_name, file_name)
                df = spark_session.read.json(file_path)
                df = df.limit(100)
                df = df.select(col("complaint_id"), col("product"),
                               col("sub_product"), col("zip_code"),
                               col("date_received"), col("consumer_disputed"),
                               col("state"), col("company_public_response"),
                               col("date_sent_to_company"),
                               col("issue"), col("sub_issue")
                               )

                transform_file_name = f"{file_name.split('.')[0]}.parquet"
                transform_file_path = os.path.join(self.transform_config.transform_dir, transform_file_name)

                df.write.parquet(transform_file_path)
            transform_output = TransformOutput(transform_dir=self.transform_config.transform_dir)
            logging.info(transform_output)
            return transform_output
        except Exception as e:
            raise e


if __name__ == "__main__":
    import json,sys,os

    config = json.loads(sys.argv[1])
    extract_output = config[constant.OUTPUT_KEY_NAME][constant.EXTRACT_OUTPUT_KEY]
    transform_config = config[constant.CONFIG_KEY_NAME][constant.TRANSFORM_CONFIG_KEY_NAME]

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
    json.dump(config, open(os.path.join("data", "report.json"), "w"),indent=4)
