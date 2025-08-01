import os
import shutil

from tqdm import tqdm

from converter.abstract_converter import AbstractConverter
from utils.logger import create_logger
from utils.mysql_connector import Database


class ViewConverter(AbstractConverter):

    def __init__(self):
        super().__init__()
        self.logger = create_logger('view_converter')
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='orienlink_batch_storage'
        )


    # mysql> desc t_node_tree;  TODO t_data_source 里没有配置文件数据，暂时不动
    # +---------------+---------------+------+-----+-------------------+-----------------------------+
    # | Field         | Type          | Null | Key | Default           | Extra                       |
    # +---------------+---------------+------+-----+-------------------+-----------------------------+
    # | id            | int(11)       | NO   | PRI | NULL              | auto_increment              |
    # | space_id      | int(11)       | YES  |     | NULL              |                             |
    # | parent_id     | int(11)       | YES  |     | NULL              |                             |
    # | type          | int(11)       | YES  |     | NULL              |                             |
    # | node_type     | int(11)       | YES  |     | NULL              |                             |
    # | template_type | int(11)       | YES  |     | NULL              |                             |
    # | view_id       | int(11)       | YES  |     | NULL              |                             |
    # | name          | varchar(128)  | YES  |     | NULL              |                             |
    # | position      | varchar(64)   | YES  |     | NULL              |                             |
    # | template_url  | varchar(1024) | YES  |     | NULL              |                             |
    # | minio_id      | varchar(64)   | YES  |     | NULL              |                             |
    # | create_user   | int(11)       | YES  |     | NULL              |                             |
    # | update_user   | int(11)       | YES  |     | NULL              |                             |
    # | create_time   | datetime      | YES  |     | CURRENT_TIMESTAMP |                             |
    # | update_time   | datetime      | YES  |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    # | lock_status   | tinyint(4)    | YES  |     | NULL              |                             |
    # | file_info     | json          | YES  |     | NULL              |                             |
    # +---------------+---------------+------+-----+-------------------+-----------------------------+
    # 17 rows in set (0.00 sec)
    def convert(self):
        connection = self.db.connect()
        results = self.db.query(connection,"SELECT id, template_url,space_id FROM t_node_tree")

        with tqdm(total=len(results), desc="video sync", unit="file") as pbar:
            try:
                for row in results:
                    download_path = ''
                    try:
                        id = row[0]
                        template_url = row[1]
                        space_id = row[2]

                        file_name = os.path.basename(template_url)
                        download_path = f'tmp/view/{id}/{file_name}'

                        download_success=self.dfs.download_file_by_url(template_url, download_path)
                        if not download_success:
                            self.logger.error(f"id {id} download error: {template_url}")
                            pbar.update(1)
                            continue

                        upload_resp_json = self.fs.upload(space_id, download_path, file_name, 'dataview')
                        if "error" in upload_resp_json or not upload_resp_json.get('data') :
                            self.logger.error(f"id {id} upload error: {upload_resp_json}")
                            pbar.update(1)
                            continue
                        file_info = upload_resp_json.get('data')

                        sql = 'UPDATE t_node_tree SET file_info = %s FROM t_node_tree WHERE id = %s'
                        params = (file_info, id)
                        self.db.execute(connection, sql, params)
                        pbar.update(1)
                    finally:
                        if download_path and os.path.exists(download_path):
                            shutil.rmtree(os.path.dirname(download_path))
                connection.commit()
            except Exception as e:
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                connection.close()
