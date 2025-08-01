import toml


class ConfigReader:

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config_data = self._read_config()

    def _read_config(self):
        """读取并解析 toml 配置文件"""
        try:
            with open(self.config_path, 'r') as f:
                return toml.load(f)
        except Exception as e:
            print(f"Error reading config file: {e}")
            return {}

    def get_fs_api(self):
        """获取 fs API 配置"""
        fs_config = self.config_data.get('fs', {})
        return {
            'fs_upload_api': fs_config.get('fs_upload_api', ''),
            'fs_client_api': fs_config.get('fs_client_api', ''),
            'default_space_id': fs_config.get('default_space_id', 1),
            'fs_read_bucket': fs_config.get('fs_read_bucket', ''),
        }

    def get_mysql_config(self):
        """获取 MySQL 配置信息"""
        mysql_config = self.config_data.get('mysql', {})
        return {
            'host': mysql_config.get('host', ''),
            'port': mysql_config.get('port', ''),
            'user': mysql_config.get('user', ''),
            'password': mysql_config.get('password', '')
        }

    def get_gofast_config(self):
        """获取 gofast 配置"""
        return self.config_data.get('gofast', {}).get('host')


# 使用示例
if __name__ == "__main__":
    config_reader = ConfigReader("../conf/conf.toml")  # 请替换为你实际的 toml 文件路径

    # 获取 fs_api 配置
    fs_api = config_reader.get_fs_api()
    print(f"FS API: {fs_api}")

    # 获取 mysql 配置
    mysql_config = config_reader.get_mysql_config()
    print(f"MySQL Config: {mysql_config}")
