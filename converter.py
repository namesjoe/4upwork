import json
import csv
import shutil
import pandas as pd
from Logger import logger

class Converter:
    _sep = ';'

    def convert_csv_to_json(self, csv_file, json_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f, delimiter=self._sep)
            columns = next(reader)
            data = []
            for row in reader:
                dummy = {k: '' for k in columns}
                for ix, col in enumerate(columns):
                    dummy[col] = row[ix]
                data.append(dummy)
        logger.info(f"csv contains {len(data)} rows")
        with open(json_file, 'w') as f:
            json.dump(data, f)
        logger.info(f"data dumped to json_file {json_file}")



    def convert_json_to_csv(self, json_file, csv_file):
        logger.info(f"Starting convert of json:{json_file} to csv:{csv_file}")
        data = pd.read_json(json_file)
        logger.info(f"json contains {data.shape[0]} rows")
        data.to_csv(csv_file, index=False, sep=self._sep)
        logger.info(f"data dumped to csv_file {csv_file}")


    @staticmethod
    def move_file_wo_convertion(source, target):
        shutil.copy(source, target)
        logger.info(f"File {source} moved to {target}")

    def convert(self, source_path, source_type, target_path, target_type):
        if source_type == target_type:
            logger.info(f"Moving file wo convertion")
            self.move_file_wo_convertion(source_path, target_path)
        elif (source_type, target_type) == ('csv', 'json'):
            logger.info(f"Converting from csv to json")
            self.convert_csv_to_json(source_path, target_path)
        elif (source_type, target_type) == ('json', 'csv'):
            logger.info(f"Converting from json to csv")
            self.convert_json_to_csv(source_path, target_path)
        else:
            raise NotImplementedError(f'Such combination is not implemented {(source_type, target_type)}')
        return 1

