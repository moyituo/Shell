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
        self.logger = create_logger('aml_converter')
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='adhere_share'
        )

    # mysql> desc t_orienlink_program;
    # +-----------------------+---------------+------+-----+-------------------+-----------------------------+
    # | Field                 | Type          | Null | Key | Default           | Extra                       |
    # +-----------------------+---------------+------+-----+-------------------+-----------------------------+
    # | id                    | int(11)       | NO   | PRI | NULL              | auto_increment              |
    # | user_id               | int(11)       | NO   |     | 1                 |                             |
    # | user_name             | varchar(64)   | YES  |     | NULL              |                             |
    # | create_type           | int(11)       | NO   |     | NULL              |                             |
    # | program_name          | varchar(6000) | NO   |     | NULL              |                             |
    # | file_name             | varchar(64)   | YES  |     | NULL              |                             |
    # | program_language_type | int(11)       | YES  |     | NULL              |                             |
    # | space_id              | int(11)       | YES  |     | NULL              |                             |
    # | cal_stage             | int(11)       | YES  |     | NULL              |                             |
    # | input_type            | int(11)       | YES  |     | NULL              |                             |
    # | program_urls          | json          | YES  |     | NULL              |                             |
    # | common_type           | int(11)       | YES  |     | NULL              |                             |
    # | communication_type    | varchar(64)   | YES  |     | NULL              |                             |
    # | model_column          | varchar(45)   | YES  |     | NULL              |                             |
    # | extract_detail_data   | json          | YES  |     | NULL              |                             |
    # | model_type            | varchar(45)   | YES  |     | NULL              |                             |
    # | create_time           | datetime      | YES  |     | CURRENT_TIMESTAMP |                             |
    # | update_time           | datetime      | YES  |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    # | program_description   | varchar(1000) | YES  |     | NULL              |                             |
    # | file_id               | int(11)       | YES  |     | NULL              |                             |
    # | program_info_md5      | varchar(225)  | YES  |     | NULL              |                             |
    # | lock_status           | tinyint(4)    | YES  |     | NULL              |                             |
    # | modify_user_id        | int(11)       | YES  |     | NULL              |                             |
    # | modify_user_name      | varchar(64)   | YES  |     | NULL              |                             |
    # +-----------------------+---------------+------+-----+-------------------+-----------------------------+
    # 24 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection,"SELECT id, file_id, file_name, program_urls, space_id FROM t_orienlink_program")

        with tqdm(total=len(results), desc="aml sync", unit="file") as pbar:
            try:
                connection.begin()
                for row in results:
                    download_path=''
                    attachment_file_path=''
                    try:
                        id = row[0]
                        program_urls = row[3]
                        space_id = row[4]

                        program_urls_list = json.loads(program_urls)

                        if program_urls_list:
                            first_url = program_urls_list[0]
                        else:
                            self.logger.error(f"ID: {id}, lack urls: {program_urls}")
                            pbar.update(1)
                            continue

                        program_file_name = os.path.basename(first_url)
                        download_path = f'tmp/aml/{id}/{program_file_name}'
                        download_success=self.dfs.download_file_by_url(first_url, download_path)
                        if not download_success:
                            self.logger.error(f"ID: {id}, Download failed for URL: {first_url}")
                            pbar.update(1)
                            continue

                        upload_resp_json = self.fs.upload(space_id, download_path, program_file_name, 'operator')
                        if "error" in upload_resp_json or not upload_resp_json.get('data'):
                            self.logger.error(f"ID: {id}, Upload error: {upload_resp_json}")
                            pbar.update(1)
                            continue
                        program_file_info = upload_resp_json.get('data')

                        # 处理附件文件
                        file_id = row[1]
                        attachment_file_name = row[2]
                        attachment_file_info = None
                        if file_id and attachment_file_name:
                            attachment_file_path = f'tmp/aml_attachment/{id}/{attachment_file_name}'
                            download_success=self.dfs.dfs(file_id, attachment_file_path)
                            if not download_success:
                                self.logger.error(f"ID: {id}, Attachment download failed for file_id: {file_id}")
                                pbar.update(1)
                                continue
                            upload_resp_json = self.fs.upload(space_id, attachment_file_path, attachment_file_name,
                                                              'operator/attachment')
                            if "error" in upload_resp_json or not upload_resp_json.get('data'):
                                self.logger.error(f"ID: {id}, File_Id: {file_id} , Upload attachment error: {upload_resp_json}")
                                pbar.update(1)
                                continue
                            else:
                                attachment_file_info = upload_resp_json.get('data')

                        # 拼接SQL语句
                        sql = 'UPDATE t_orienlink_program SET program_file_info = %s'
                        params = [json.dumps(program_file_info)]  # 初始化参数

                        # 如果有附件文件，追加更新语句
                        if attachment_file_info:
                            sql += ', upload_file_info = %s'
                            params.append(json.dumps(attachment_file_info))

                        # 完成WHERE条件
                        sql += ' WHERE id = %s'
                        params.append(id)

                        # 执行更新
                        self.db.execute(connection, sql, tuple(params))

                        # ---------------------------------------清理文件--------------------------------------------
                        pbar.update(1)
                    finally:
                        if download_path and os.path.exists(download_path):
                            shutil.rmtree(os.path.dirname(download_path))
                        if attachment_file_path and attachment_file_info and os.path.exists(attachment_file_path):
                            shutil.rmtree(os.path.dirname(attachment_file_path))
                connection.commit()
            except Exception as e:
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                connection.close()

