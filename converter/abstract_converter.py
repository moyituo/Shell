import hashlib
from abc import ABC, abstractmethod

from utils.toml_reader import ConfigReader
from utils.dfs_file_info import DFS
from utils.upload_file import FileUploader


class AbstractConverter(ABC):

    def __init__(self):
        self.config_reader = ConfigReader("conf/conf.toml")
        self.mysql_config = self.config_reader.get_mysql_config()
        self.dfs = DFS()
        self.fs_config=self.config_reader.get_fs_api()
        self.fs = FileUploader(self.fs_config.get('fs_upload_api'),self.fs_config.get('fs_client_api'))

    @abstractmethod
    def convert(self):
        pass

    @staticmethod
    def calculate_file_md5(file_path: str) -> str:
        """计算文件内容的 MD5 值"""
        md5_hash = hashlib.md5()  # 创建一个 MD5 哈希对象

        with open(file_path, 'rb') as file:  # 以二进制模式打开文件
            # 分块读取文件内容，避免大文件一次性加载到内存
            while chunk := file.read(8192):  # 逐块读取文件，默认为 8KB
                md5_hash.update(chunk)  # 更新 MD5 哈希值

        return md5_hash.hexdigest()  # 返回 MD5 哈希值的十六进制表示
