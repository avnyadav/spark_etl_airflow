from six.moves import urllib
import os
from zipfile import ZipFile
from collections import namedtuple
from etl_project.finance_complaint_config.config import ExtractConfig

ExtractOutput = namedtuple("Extractoutput", ["extract_dir"])
import logging

class Extract:
    def __init__(self, extract_config: ExtractConfig):
        self.extract_config = extract_config
        logging.info(self.extract_config)

    def download_file(self):
        try:
            # creating download directory

            download_dir = self.extract_config.download_dir
            os.makedirs(download_dir, exist_ok=True)

            # preparing file name for downloaded file
            url = self.extract_config.download_url
            file_name = os.path.basename(url)

            # complete file path for download file
            download_file_path = os.path.join(download_dir, file_name)
            urllib.request.urlretrieve(url, download_file_path)
            return download_file_path
        except Exception as e:
            raise e

    def extract_file(self, zip_file) -> ExtractOutput:
        try:
            # creating extract directory
            os.makedirs(self.extract_config.extract_dir, exist_ok=True)

            #extracting file
            with ZipFile(zip_file, ) as zipfile:
                zipfile.extractall(path=self.extract_config.extract_dir)

            extract_output = ExtractOutput(extract_dir=self.extract_config.extract_dir, )
            print(extract_output)
            return extract_output
        except  Exception as e:
            raise e

    def start_extraction(self):
        try:
            downloaded_file_path = self.download_file()
            return self.extract_file(zip_file=downloaded_file_path)
        except Exception as e:
            raise e
