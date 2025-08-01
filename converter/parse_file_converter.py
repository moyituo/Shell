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
            database='adhere_testproject'
        )

    # mysql> desc t_parse_file;
    # +----------------+--------------+------+-----+-------------------+----------------+
    # | Field          | Type         | Null | Key | Default           | Extra          |
    # +----------------+--------------+------+-----+-------------------+----------------+
    # | id             | int(11)      | NO   | PRI | NULL              | auto_increment |
    # | file_source_id | int(11)      | NO   |     | NULL              |                |
    # | file_name      | varchar(255) | NO   |     | NULL              |                |
    # | type           | tinyint(4)   | YES  |     | NULL              |                |
    # | status         | tinyint(4)   | NO   |     | NULL              |                |
    # | error_msg      | varchar(512) | YES  |     | NULL              |                |
    # | path           | varchar(512) | NO   |     | NULL              |                |
    # | md5            | varchar(45)  | NO   | MUL | NULL              |                |
    # | name_space_id  | varchar(64)  | YES  |     | NULL              |                |
    # | create_time    | datetime     | NO   |     | CURRENT_TIMESTAMP |                |
    # | update_time    | datetime     | NO   |     | CURRENT_TIMESTAMP |                |
    # | is_some_ip     | tinyint(4)   | YES  |     | 0                 |                |
    # +----------------+--------------+------+-----+-------------------+----------------+
    # 12 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection, "SELECT id, file_name, file_source_id,md5 FROM t_parse_file")

        with tqdm(total=len(results), desc="parse file sync", unit="file") as pbar:
            try:
                connection.begin()  # 开始事务
                for row in results:
                    download_path = ''
                    try:
                        id = row[0]
                        file_name = row[1]
                        file_source_id = row[2]
                        old_md5 = row[3]

                        # 下载路径
                        download_path = f'tmp/parse_file/{id}/{file_name}'
                        download_success = self.dfs.dfs(file_source_id, download_path)
                        if not download_success:
                            # 记录下载失败的详细信息
                            self.log_failure(id, file_source_id, file_name, 'Download failed',
                                             'Failed to download from DFS')
                            pbar.update(1)
                            continue  # 如果下载失败，跳过当前文件

                        # TODO
                        default_space_id = self.config_reader.get_fs_api()['default_space_id']
                        upload_resp_json = self.fs.upload(default_space_id, download_path, file_name, 'parse_files')
                        if "error" in upload_resp_json or not upload_resp_json.get('data'):
                            # 记录上传失败的详细信息
                            self.log_failure(id, file_source_id, file_name, 'Upload failed', upload_resp_json)
                            pbar.update(1)
                            continue  # 如果上传失败，跳过当前文件

                        # 上传成功，更新数据库
                        file_info = upload_resp_json.get('data')
                        file_name = file_info.get('fileName')

                        sql = 'UPDATE t_parse_file SET file_name = %s, file_info = %s WHERE id = %s'
                        params = (file_name, file_info, id)
                        self.db.execute(connection, sql, params)
                        pbar.update(1)
                    finally:
                        if download_path and os.path.exists(download_path):
                            shutil.rmtree(os.path.dirname(download_path))
                # 提交事务
                connection.commit()
            except Exception as e:
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                connection.close()

    def log_failure(self, id: int, file_id: int, file_name: str, error_type: str, error_msg: str):
        """记录失败的文件信息到日志文件"""
        log_msg = f"ID: {id}, File_Id: {file_id} ,File Name: {file_name}, Error Type: {error_type}, Error Msg: {error_msg}"
        self.logger.error(log_msg)
