import os
import shutil

from tqdm import tqdm

from converter.abstract_converter import AbstractConverter
from utils.logger import create_logger
from utils.mysql_connector import Database


class SimulinkConverter(AbstractConverter):

    def __init__(self):
        super().__init__()
        self.logger = create_logger('simulink_converter')
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='adhere_signal_mapping'
        )

    # mysql> desc t_simulink_file_info;
    # +--------------------+---------------+------+-----+-------------------+-----------------------------+
    # | Field              | Type          | Null | Key | Default           | Extra                       |
    # +--------------------+---------------+------+-----+-------------------+-----------------------------+
    # | id                 | int(11)       | NO   | PRI | NULL              | auto_increment              |
    # | vehicle_id         | int(11)       | YES  |     | NULL              |                             |
    # | file_id            | int(11)       | YES  |     | NULL              |                             |
    # | file_name          | varchar(256)  | NO   |     | NULL              |                             |
    # | type               | int(4)        | NO   |     | NULL              |                             |
    # | status             | int(4)        | NO   |     | 1                 |                             |
    # | error_msg          | varchar(1024) | YES  |     | NULL              |                             |
    # | valid              | int(4)        | NO   |     | NULL              |                             |
    # | create_time        | datetime      | NO   |     | CURRENT_TIMESTAMP |                             |
    # | update_time        | datetime      | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    # | model_check_status | int(4)        | YES  |     | NULL              |                             |
    # +--------------------+---------------+------+-----+-------------------+-----------------------------+
    # 11 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection, "SELECT id,file_id,file_name FROM t_simulink_file_info")

        with tqdm(total=len(results), desc="simulink file sync", unit="file") as pbar:
            try:
                connection.begin()
                for row in results:
                    download_path=''
                    try:
                        id = row[0]
                        file_id = row[1]
                        file_name = row[2]

                        download_path = f'tmp/simulink/{file_id}/{file_name}'
                        download_success=self.dfs.dfs(file_id, download_path)
                        if not download_success:
                            self.logger.error(f"ID: {id}, File_Id: {file_id} ,File Name: {file_name}, Download failed")
                            pbar.update(1)
                            continue
                        # TODO
                        default_space_id = self.config_reader.get_fs_api()['default_space_id']
                        upload_resp_json = self.fs.upload(default_space_id, download_path, file_name, 'models')
                        if "error" in upload_resp_json or not upload_resp_json.get('data'):
                            self.logger.error(f"ID: {id}, File_Id: {file_id} ,File Name: {file_name}, Error Msg: {upload_resp_json}")
                            pbar.update(1)
                            continue
                        file_info = upload_resp_json.get('data')
                        file_name = file_info.get('fileName')

                        sql = 'UPDATE t_simulink_file_info SET file_name = %s ,simulink_file_info = %s FROM t_simulink_file_info WHERE id = %s'
                        params = (file_name, file_info, id)
                        self.db.execute(connection, sql, params)
                        pbar.update(1)
                    finally:
                        if download_path and os.path.exists(download_path):
                            shutil.rmtree(os.path.dirname(download_path))
                connection.commit()
            except Exception as e:
                # 如果发生异常，回滚事务，并记录详细信息
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                # 关闭数据库连接
                connection.close()
