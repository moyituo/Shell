import json
import os
import shutil
from tqdm import tqdm
from elasticsearch import Elasticsearch

from converter.abstract_converter import AbstractConverter
from utils.logger import create_logger
from utils.mysql_connector import Database


class ParseFileConverter(AbstractConverter):
    """处理程序文件转换的类，负责从数据库获取程序信息并上传到文件存储系统"""

    def __init__(self):
        """初始化转换器，设置日志、数据库连接和ES连接"""
        super().__init__()
        # 创建专用日志记录器
        self.logger = create_logger('aml_converter')
        # 初始化数据库连接，使用配置中的MySQL参数
        self.db = Database(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database='adhere_share'  # 固定连接adhere_share数据库
        )
        # 初始化Elasticsearch连接
        self.es = Elasticsearch(
            hosts=[self.es_config['host']],
            port=self.es_config['port']
        )
        self.es_index = "algorithm"  # ES索引名称

    def _update_es(self, algorithm_id, program_file_info=None, upload_file_info=None):
        """更新Elasticsearch中的算法文档

        Args:
            algorithm_id: 算法ID，作为ES文档ID
            program_file_info: 程序文件信息(JSON)
            upload_file_info: 上传文件信息(JSON)
        """
        doc = {}
        if program_file_info:
            doc['program_file_info'] = program_file_info
        if upload_file_info:
            doc['upload_file_info'] = upload_file_info

        if doc:  # 只有有数据需要更新时才执行
            try:
                self.es.update(
                    index=self.es_index,
                    id=algorithm_id,
                    body={"doc": doc}
                )
                self.logger.debug(f"Successfully updated ES document {algorithm_id}")
            except Exception as e:
                self.logger.error(f"Failed to update ES document {algorithm_id}: {str(e)}")

    def convert(self):
        """主转换方法，执行完整的转换流程"""
        # 获取数据库连接
        connection = self.db.connect()
        # 查询需要处理的程序记录，只选择必要的字段
        results = self.db.query(connection,
                                "SELECT id, file_id, file_name, program_urls, space_id FROM t_orienlink_program")

        # 使用进度条显示处理进度
        with tqdm(total=len(results), desc="aml sync", unit="file") as pbar:
            try:
                # 开始数据库事务
                connection.begin()
                for row in results:
                    # 初始化文件路径变量，用于后续清理
                    download_path = ''
                    attachment_file_path = ''
                    try:
                        # 解构查询结果
                        id = row[0]  # 程序ID
                        file_id = row[1]  # 附件文件ID
                        attachment_file_name = row[2]  # 附件文件名
                        program_urls = row[3]  # 程序URL列表(JSON格式)
                        space_id = row[4]  # 空间ID

                        # 解析程序URL列表
                        program_urls_list = json.loads(program_urls)

                        # 检查URL列表是否为空
                        if program_urls_list:
                            first_url = program_urls_list[0]  # 只处理第一个URL
                        else:
                            self.logger.error(f"ID: {id}, lack urls: {program_urls}")
                            pbar.update(1)
                            continue  # 跳过没有URL的记录

                        # 从URL中提取文件名
                        program_file_name = os.path.basename(first_url)
                        # 设置程序文件下载路径
                        download_path = f'tmp/aml/{id}/{program_file_name}'

                        # 下载程序文件
                        download_success = self.dfs.download_file_by_url(first_url, download_path)
                        if not download_success:
                            self.logger.error(f"ID: {id}, Download failed for URL: {first_url}")
                            pbar.update(1)
                            continue  # 下载失败则跳过

                        # 上传程序文件到文件存储系统
                        upload_resp_json = self.fs.upload(space_id, download_path, program_file_name, 'operator')
                        if "error" in upload_resp_json or not upload_resp_json.get('data'):
                            self.logger.error(f"ID: {id}, Upload error: {upload_resp_json}")
                            pbar.update(1)
                            continue  # 上传失败则跳过
                        program_file_info = upload_resp_json.get('data')  # 获取上传后的文件信息

                        # 处理附件文件（如果存在）
                        attachment_file_info = None
                        if file_id and attachment_file_name:
                            # 设置附件文件下载路径
                            attachment_file_path = f'tmp/aml_attachment/{id}/{attachment_file_name}'
                            # 下载附件文件
                            download_success = self.dfs.dfs(file_id, attachment_file_path)
                            if not download_success:
                                self.logger.error(f"ID: {id}, Attachment download failed for file_id: {file_id}")
                                pbar.update(1)
                                continue  # 附件下载失败则跳过

                            # 上传附件文件到文件存储系统
                            upload_resp_json = self.fs.upload(space_id, attachment_file_path, attachment_file_name,
                                                              'operator/attachment')
                            if "error" in upload_resp_json or not upload_resp_json.get('data'):
                                self.logger.error(
                                    f"ID: {id}, File_Id: {file_id} , Upload attachment error: {upload_resp_json}")
                                pbar.update(1)
                                continue  # 附件上传失败则跳过
                            else:
                                attachment_file_info = upload_resp_json.get('data')  # 获取附件文件信息

                        # 构建更新数据库的SQL语句
                        sql = 'UPDATE t_orienlink_program SET program_file_info = %s'  # 基础更新语句
                        params = [json.dumps(program_file_info)]  # 初始化参数列表

                        # 如果有附件文件信息，添加到更新语句
                        if attachment_file_info:
                            sql += ', upload_file_info = %s'
                            params.append(json.dumps(attachment_file_info))

                        # 添加WHERE条件
                        sql += ' WHERE id = %s'
                        params.append(id)

                        # 执行数据库更新
                        self.db.execute(connection, sql, tuple(params))

                        # 更新Elasticsearch中的记录
                        self._update_es(
                            algorithm_id=id,
                            program_file_info=json.dumps(program_file_info),
                            upload_file_info=json.dumps(attachment_file_info)
                        )

                        # 更新进度条
                        pbar.update(1)
                    finally:
                        # 清理临时文件
                        if download_path and os.path.exists(download_path):
                            shutil.rmtree(os.path.dirname(download_path))  # 删除程序文件临时目录
                        if attachment_file_path and attachment_file_info and os.path.exists(attachment_file_path):
                            shutil.rmtree(os.path.dirname(attachment_file_path))  # 删除附件文件临时目录

                # 所有记录处理完成，提交事务
                connection.commit()
            except Exception as e:
                # 发生异常时回滚事务并记录错误
                self.logger.error(f"Exception occurred during conversion: {e}")
                connection.rollback()
            finally:
                # 确保数据库连接被关闭
                connection.close()
