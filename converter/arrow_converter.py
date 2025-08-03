import json
import os
import shutil

from tqdm import tqdm

from converter.abstract_converter import AbstractConverter
from utils.logger import create_logger
from utils.mysql_connector import Database


class ArrowConverter(AbstractConverter):

    def __init__(self):
        super().__init__()
        self.logger = create_logger('arrow_converter')
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='orienlink_batch_storage'
        )

    def upload_file(self, space_id: int, file_path: str, file_name: str, folder: str, original: bool) -> dict:
        """
        上传文件并返回响应数据
        :param original:
        :param space_id: 空间ID
        :param file_path: 本地文件路径
        :param file_name: 文件名
        :param folder: 文件夹路径
        :return: 上传响应的数据
        """
        upload_resp_json = self.fs.upload(space_id, file_path, file_name, folder, original)
        if "error" in upload_resp_json or not upload_resp_json.get('data'):
            self.logger.error(f"File upload failed: {upload_resp_json}")
            return None
        else:
            return upload_resp_json.get('data')

    # mysql> desc t_arrow_file;
    # +---------------------+--------------+------+-----+-------------------+-----------------------------+
    # | Field               | Type         | Null | Key | Default           | Extra                       |
    # +---------------------+--------------+------+-----+-------------------+-----------------------------+
    # | id                  | int(11)      | NO   | PRI | NULL              | auto_increment              |
    # | space_id            | int(11)      | NO   |     | NULL              |                             |
    # | origin_file_id      | bigint(20)   | YES  | MUL | NULL              |                             |
    # | origin_file_name    | varchar(255) | NO   |     | NULL              |                             |
    # | origin_file_info    | json         | YES  |     | NULL              |                             |
    # | channel_config_md5  | varchar(64)  | YES  |     | NULL              |                             |
    # | channel_config_url  | text         | YES  |     | NULL              |                             |
    # | start_time          | bigint(20)   | NO   |     | NULL              |                             |
    # | end_time            | bigint(20)   | NO   |     | NULL              |                             |
    # | file_date           | varchar(10)  | YES  | MUL | NULL              |                             |
    # | arrow_file_info     | json         | YES  |     | NULL              |                             |
    # | create_time         | datetime     | NO   |     | CURRENT_TIMESTAMP |                             |
    # | update_time         | datetime     | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    # | ol_arrow_file_info  | json         | YES  |     | NULL              |                             |
    # | ol_origin_file_info | json         | YES  |     | NULL              |                             |
    # +---------------------+--------------+------+-----+-------------------+-----------------------------+
    # 15 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection,
                                "SELECT id, origin_file_info, channel_config_url, arrow_file_info,space_id FROM t_arrow_file")

        with tqdm(total=len(results), desc="arrow files", unit="file") as pbar:
            try:
                connection.begin()
                for row in results:
                    origin_download_path = ''
                    arrow_download_path = ''
                    try:
                        id = row[0]
                        space_id = row[4]
                        sql = 'UPDATE t_arrow_file SET '  # 开始拼接 SQL 语句
                        params = []  # 用来存放 SQL 参数

                        # -------------------------------origin_file_info---------------------------------
                        origin_file_info_json = json.loads(row[1])
                        origin_file_info_path = origin_file_info_json.get('path')
                        origin_file_name = os.path.basename(origin_file_info_path)
                        origin_download_path = f'tmp/arrow_origin/{id}/{origin_file_name}'

                        download_success = self.dfs.download_file_by_url(origin_file_info_path, origin_download_path)
                        if not download_success:
                            self.logger.error(f"id {id} 原始文件下载失败")
                            pbar.update(1)
                            continue

                        origin_file_info_path = origin_file_info_path.replace(
                            'http://10.10.3.13:18082/group1/originalData/', '')
                        origin_file_info_path = origin_file_info_path.replace(
                            'http://10.10.3.13:18082/group1/localUpload/', '')
                        origin_file_info_path = self.config_reader.get_fs_api()['fs_read_bucket'] + '/' + '/'.join(
                            origin_file_info_path.split('/')[:-1])
                        print("1112123123123123123123123211")
                        print(origin_file_info_path)
                        file_info = self.upload_file(space_id, origin_download_path, origin_file_name,
                                                     origin_file_info_path, True)
                        if file_info is None:
                            self.logger.error(f"id {id} 原始文件上传失败")
                            pbar.update(1)
                            continue

                        origin_file_name = file_info.get('fileName')
                        sql += 'origin_file_name = %s, ol_origin_file_info = %s'  # 拼接字段
                        params.append(origin_file_name)
                        params.append(json.dumps(file_info))

                        # ------------------------------arrow_file_info--------------------------------
                        if row[3]:  # 如果有 arrow_file_info
                            arrow_file_info_json = json.loads(row[3])
                            arrow_file_info_path = arrow_file_info_json.get('path')
                            arrow_file_name = arrow_file_info_path.split('name=')[1].split('&')[0]

                            arrow_download_path = f'tmp/arrow_file/{id}/{arrow_file_name}'

                            download_success = self.dfs.download_file_by_url(arrow_file_info_path, arrow_download_path)
                            if not download_success:
                                self.logger.error(f"id {id} arrow文件下载失败")
                                pbar.update(1)
                                continue

                            arrow_file_info_path = "arrows/" + '/'.join(origin_file_info_path.split('/')[:-1])

                            print('--==================')
                            print(arrow_file_name)
                            print(arrow_file_info_path)
                            arrow_file_info = self.upload_file(space_id, arrow_download_path, arrow_file_name,
                                                         arrow_file_info_path, False)
                            if arrow_file_info is None :
                                print("============+++++++++")
                                self.logger.error(f"id {id} arrow文件上传失败")
                                pbar.update(1)
                                continue

                            sql += ', ol_arrow_file_info = %s'
                            params.append(json.dumps(arrow_file_info))

                        # 添加 WHERE 子句
                        sql += ' WHERE id = %s'
                        params.append(id)

                        # 执行 SQL 更新
                        self.db.execute(connection, sql, tuple(params))
                        pbar.update(1)

                    finally:
                        if origin_download_path and os.path.exists(origin_download_path):
                            shutil.rmtree(os.path.dirname(origin_download_path))
                        if arrow_download_path and os.path.exists(arrow_download_path):
                            shutil.rmtree(os.path.dirname(arrow_download_path))
                connection.commit()
            except Exception as e:
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                connection.close()
