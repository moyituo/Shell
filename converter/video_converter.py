import json
import os
import shutil
from tqdm import tqdm
from converter.abstract_converter import AbstractConverter
from utils.logger import create_logger
from utils.mysql_connector import Database


class VideoConverter(AbstractConverter):

    def __init__(self):
        super().__init__()
        self.logger = create_logger('video_converter')
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='orienlink_batch_storage'
        )

    # TODO 需要清除 t_video_save 中的所有数据
    # mysql> desc t_time_sequence_object;
    # +---------------------+--------------+------+-----+-------------------+-----------------------------+
    # | Field               | Type         | Null | Key | Default           | Extra                       |
    # +---------------------+--------------+------+-----+-------------------+-----------------------------+
    # | id                  | int(11)      | NO   | PRI | NULL              | auto_increment              |
    # | work_flow_id        | int(11)      | NO   |     | NULL              |                             |
    # | space_id            | int(11)      | NO   |     | NULL              |                             |
    # | project_id          | int(11)      | NO   |     | NULL              |                             |
    # | vehicle_id          | int(11)      | NO   |     | NULL              |                             |
    # | channel_type        | tinyint(4)   | NO   |     | 1                 |                             |
    # | channel_id          | int(11)      | NO   |     | NULL              |                             |
    # | channel             | int(11)      | YES  |     | NULL              |                             |
    # | channel_info        | json         | YES  |     | NULL              |                             |
    # | status              | tinyint(4)   | NO   |     | 0                 |                             |
    # | start_timestamp     | bigint(8)    | NO   |     | 0                 |                             |
    # | end_timestamp       | bigint(8)    | NO   |     | 0                 |                             |
    # | origin_file_id      | int(11)      | YES  |     | NULL              |                             |
    # | file_name           | varchar(128) | YES  |     | NULL              |                             |
    # | table_name          | varchar(128) | YES  |     | NULL              |                             |
    # | type                | tinyint(4)   | NO   |     | 1                 |                             |
    # | poster_url          | varchar(255) | YES  |     | NULL              |                             |
    # | create_time         | datetime     | NO   |     | CURRENT_TIMESTAMP |                             |
    # | update_time         | datetime     | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    # | file_path           | varchar(255) | YES  |     | NULL              |                             |
    # | video_name_space_id | varchar(45)  | YES  |     | NULL              |                             |
    # | upload_type         | int(4)       | YES  |     | NULL              |                             |
    # | minio_id            | varchar(64)  | YES  |     | NULL              |                             |
    # | name_space_id       | varchar(64)  | YES  |     | NULL              |                             |
    # | file_system_type    | varchar(20)  | YES  |     | NULL              |                             |
    # | pic_table_name      | varchar(64)  | YES  |     | NULL              |                             |
    # | link_jira_error     | tinyint(4)   | YES  |     | NULL              |                             |
    # | run_id              | varchar(225) | YES  |     | NULL              |                             |
    # | post_file_info      | json         | YES  |     | NULL              |                             |
    # | video_file_info     | json         | YES  |     | NULL              |                             |
    # | origin_file_info    | json         | YES  |     | NULL              |                             |
    # +---------------------+--------------+------+-----+-------------------+-----------------------------+
    # 31 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection, "SELECT id, poster_url, file_path, space_id FROM t_time_sequence_object")

        with tqdm(total=len(results), desc="video sync", unit="file") as pbar:
            try:
                connection.begin()  # 开始事务
                for row in results:
                    poster_download_path = ''
                    video_download_path = ''
                    try:
                        id = row[0]
                        poster_url = row[1]
                        file_path = row[2]
                        space_id = row[3]

                        update_sql = 'UPDATE t_time_sequence_object SET '
                        params = []

                        # ---------------------------poster_url-------------------------------
                        if poster_url:
                            poster_name = os.path.basename(poster_url)
                            poster_download_path = f'tmp/video_poster/{id}/{poster_name}'
                            poster_url = poster_url.replace("/gofast", self.config_reader.get_gofast_config())
                            download_poster_success = self.dfs.download_file_by_url(poster_url, poster_download_path)
                            if not download_poster_success:
                                self.logger.error(f'id {id} poster download error: {poster_url}')
                                pbar.update(1)
                                continue

                            upload_resp_json = self.fs.upload(space_id, poster_download_path, poster_name,
                                                              'posters/sync')
                            if "error" in upload_resp_json or not upload_resp_json.get('data'):
                                self.logger.error(f'id {id} poster upload error: {upload_resp_json}')
                                pbar.update(1)
                                continue
                            file_info = upload_resp_json.get('data')
                            update_sql += 'post_file_info = %s'  # 拼接字段
                            params.append(json.dumps(file_info))

                        # --------------------------file_path---------------------------------
                        file_path_name = os.path.basename(file_path)
                        video_download_path = f'tmp/video_file/{id}/{file_path_name}'
                        file_path = file_path.replace("/gofast", self.config_reader.get_gofast_config())
                        video_download_success = self.dfs.download_file_by_url(file_path, video_download_path)
                        if not video_download_success:
                            self.logger.error(f'id {id} video download error: {file_path}')
                            pbar.update(1)
                            continue

                        upload_resp_json = self.fs.upload(space_id, video_download_path, file_path_name, 'video/sync')
                        if "error" in upload_resp_json or not upload_resp_json.get('data'):
                            self.logger.error(f'id {id} video upload error: {upload_resp_json}')
                            pbar.update(1)
                            continue
                        video_file_info = upload_resp_json.get('data')
                        if 'poster_url' in update_sql:
                            update_sql += ', '  # 如果之前已经有 poster_url 字段，则用逗号分隔
                        update_sql += 'video_file_info = %s'  # 拼接字段
                        params.append(json.dumps(video_file_info))

                        # 最后拼接 WHERE 子句
                        update_sql += ' WHERE id = %s'
                        params.append(id)

                        self.db.execute(connection, update_sql, tuple(params))
                    finally:
                        if poster_download_path and os.path.exists(poster_download_path):
                            shutil.rmtree(os.path.dirname(poster_download_path))
                        if video_download_path and os.path.exists(video_download_path):
                            shutil.rmtree(os.path.dirname(video_download_path))
                    pbar.update(1)
                # 提交事务
                connection.commit()
            except Exception as e:
                # 如果发生异常，回滚事务并记录日志
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                connection.close()


if __name__ == '__main__':
    url = "/gofast/group1/default/20250416/14/52/0/016ddb379a9184c6d5e616d7316ebf08.mp4"
    url = url.replace("/gofast", "http://10.10.3.13:18082")
    print(url)
