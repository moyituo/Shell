import os
import requests
from typing import Optional

from utils.toml_reader import ConfigReader
from utils.mysql_connector import Database


class DFS:

    def __init__(self):
        """
        初始化 DFS 模块
        """
        self.config_reader = ConfigReader("conf/conf.toml")
        mysql_config = self.config_reader.get_mysql_config()
        self.db = Database(
            host=mysql_config['host'],
            port=mysql_config['port'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database='adhere_mfs'
        )
        self.connection = self.db.connect()

    def get_url_by_file_id(self, file_id: int) -> Optional[str]:
        """根据文件ID查询数据库中对应的MD5值"""
        sql = "SELECT url FROM t_origin_file WHERE id = %s"
        results = self.db.query(self.connection, sql, (file_id,))
        if results:
            return results[0][0]  # 返回 MD5 值
        return None

    @staticmethod
    def download_file_by_url(url: str, download_path: str) -> bool:
        """通过 MD5 值下载文件"""
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            # 保存文件到本地
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            with open(download_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            print(f"File downloaded successfully to {download_path}")
            return True
        else:
            print(f"Failed to download file: {response.status_code}")
            return False

    def dfs(self, file_id: int, download_path: str) -> bool:
        """根据文件ID查询URL并下载文件"""
        url = self.get_url_by_file_id(file_id)
        if not url:
            print(f"File with ID {file_id} not found in database.")
            return False

        # 使用查询到的 url 值下载文件
        return self.download_file_by_url(url, download_path)
