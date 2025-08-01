import requests


class FileUploader:

    def __init__(self, url: str, original_url: str):
        """
        初始化上传模块
        :param url: 上传目标的URL地址
        """
        self.url = url
        self.original_url = original_url

    def upload(self, space_id: int, file_path: str, field_name: str, upload_path: str, original: bool = False) -> dict:
        """
        上传文件到指定URL
        :param original:
        :param space_id: 工作空间id
        :param file_path: 本地文件的路径
        :param field_name: 上传时字段的名称
        :param upload_path: 上传的目标目录
        :return: 服务器的响应数据
        """
        with open(file_path, 'rb') as file:
            files = {'file': (field_name, file)}
            data = {'spaceId': space_id, 'path': upload_path}
            if original:
                response = requests.post(self.original_url, files=files, data=data)
            else:
                response = requests.post(self.url, files=files, data=data)
        # 检查响应状态
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': '上传失败', 'status_code': response.status_code, 'message': response.text}
