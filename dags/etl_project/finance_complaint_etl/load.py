
from etl_project.finance_complaint_etl.transform import TransformOutput
import logging
from etl_project.finance_complaint_config.config import LoadConfig, PipelineConfig
import shutil
from datetime import datetime
import os
import logging

from collections import namedtuple

LoadOutput = namedtuple("LoadOutput", ["outbox_dir", "archive_dir"])


class Load:
    def __init__(self, load_config: LoadConfig, transform_output: TransformOutput,
                 pipeline_config: PipelineConfig) -> None:

        logging.info(f"Started loading operation")
        self.load_config = load_config
        logging.info(f"Load configuration: {self.load_config}")
        self.transform_output = transform_output
        logging.info(f"Transformation output: {self.transform_output}")
        self.pipeline_config = pipeline_config
        logging.info(f"pipeline config {self.pipeline_config}")

    def start_loading(self)->LoadOutput:
        try:
            dir_name = self.transform_output.transform_dir
            logging.info(f"Loading file from dir: {dir_name}")
            shutil.copytree(dir_name, self.load_config.load_dir)
            timestamp = datetime.now().strftime("%m_%d_%Y__%H_%M_%S")
            logging.info(f"Files copied into dir: {self.load_config.load_dir}")
            outbox_dir = os.path.join(self.load_config.outbox_dir,timestamp)
            shutil.copytree(dir_name,outbox_dir )
            logging.info(f"Files copied into dir: {self.load_config.outbox_dir}")
            archive_dir = os.path.join(self.pipeline_config.archive_dir, timestamp)
            shutil.copytree(dir_name, archive_dir)
            logging.info(f"Files archived  into dir: {archive_dir}")
            shutil.rmtree(self.pipeline_config.pipeline_dir)
            logging.info(f"Intermediate dir cleaned: {self.pipeline_config.pipeline_dir}")
            return LoadOutput(outbox_dir=outbox_dir,archive_dir=archive_dir)
        except Exception as e:
            raise e
