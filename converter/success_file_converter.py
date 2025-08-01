import json
import os
import shutil

from tqdm import tqdm

from converter.abstract_converter import AbstractConverter
from utils.logger import create_logger
from utils.mysql_connector import Database


class ParseFileConverter(AbstractConverter):

    def __init__(self):
        super().__init__()
        self.logger = create_logger('parse_file_converter')
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='orienlink_batch_storage'
        )

    # mysql> desc t_success_file;
    # +-----------------------+--------------+------+-----+-------------------+-----------------------------+
    # | Field                 | Type         | Null | Key | Default           | Extra                       |
    # +-----------------------+--------------+------+-----+-------------------+-----------------------------+
    # | id                    | int(11)      | NO   | PRI | NULL              | auto_increment              |
    # | vehicle_id            | int(11)      | YES  |     | NULL              |                             |
    # | origin_file_id        | int(11)      | YES  |     | NULL              |                             |
    # | file_name             | varchar(128) | YES  |     | NULL              |                             |
    # | start_timestamp       | bigint(20)   | YES  |     | NULL              |                             |
    # | end_timestamp         | bigint(20)   | YES  |     | NULL              |                             |
    # | minio_id              | varchar(225) | YES  |     | NULL              |                             |
    # | process_instance_id   | int(11)      | NO   |     | NULL              |                             |
    # | addition              | int(11)      | NO   |     | NULL              |                             |
    # | create_time           | datetime     | NO   |     | CURRENT_TIMESTAMP |                             |
    # | update_time           | datetime     | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    # | process_definition_id | int(11)      | YES  |     | NULL              |                             |
    # | files_md5             | varchar(45)  | YES  |     | NULL              |                             |
    # | vehicle_md5           | longtext     | YES  |     | NULL              |                             |
    # | link_jira_error       | tinyint(4)   | YES  |     | NULL              |                             |
    # | run_id                | text         | YES  |     | NULL              |                             |
    # | url                   | varchar(255) | YES  |     | NULL              |                             |
    # | origin_file_info      | json         | YES  |     | NULL              |                             |
    # +-----------------------+--------------+------+-----+-------------------+-----------------------------+
    # 18 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection, "SELECT id,origin_file_id, file_name FROM t_success_file")

        with tqdm(total=len(results), desc="success file sync", unit="file") as pbar:
            try:
                connection.begin()
                # 将查询结果按照 origin_file_id 分组为字典
                grouped_results = {}
                for row in results:
                    origin_file_id = row[1]
                    if origin_file_id not in grouped_results:
                        grouped_results[origin_file_id] = []
                    grouped_results[origin_file_id].append(row)

                for origin_file_id, rows in grouped_results.items():
                    download_path = ''
                    try:
                        file_name = rows[0][2]
                        ids = [row[0] for row in rows]

                        download_path = f'tmp/success_file/{origin_file_id}/{file_name}'

                        url = self.dfs.get_url_by_file_id(origin_file_id)
                        if not url:
                            self.logger.error(','.join(ids), origin_file_id, file_name, 'Download failed',
                                              'File not found in DFS')
                            pbar.update(len(ids))
                            continue
                        download_success = self.dfs.download_file_by_url(url, download_path)
                        if not download_success:
                            self.logger.error(','.join(ids), origin_file_id, file_name, 'Download failed',
                                              'Failed to download from DFS')
                            pbar.update(len(ids))
                            continue

                        # TODO
                        default_space_id = self.config_reader.get_fs_api()['default_space_id']

                        url = url.replace(
                            'http://10.10.3.13:18082/group1/originalData/', '')
                        url = self.config_reader.get_fs_api()['fs_read_bucket'] + '/' + '/'.join(
                            url.split('/')[:-1])
                        print(f"url: {url}")
                        upload_resp_json = self.fs.upload(default_space_id, download_path, file_name, url)

                        if "error" in upload_resp_json or not upload_resp_json.get('data'):
                            self.logger.error(','.join(ids), origin_file_id, file_name, 'Upload failed',
                                              'Failed to Upload MINIO')
                            pbar.update(len(ids))
                            continue
                        file_info = upload_resp_json.get('data')
                        file_name = file_info.get('fileName')

                        for row in rows:
                            id = row[0]
                            sql = 'UPDATE t_success_file SET file_name = %s, origin_file_info = %s WHERE id = %s'
                            params = (file_name, json.dumps(file_info), id)
                            print("----------------")
                            print(params)
                            # self.db.execute(connection, sql, params)
                            pbar.update(1)
                            break
                        break
                    finally:
                        if download_path and os.path.exists(download_path):
                            shutil.rmtree(os.path.dirname(download_path))
                connection.commit()
            except Exception as e:
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                connection.close()
