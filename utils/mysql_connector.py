import pymysql
from typing import Tuple, Any


class Database:

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        """建立数据库连接"""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )

    @staticmethod
    def query(connection, sql: str, params: Tuple = ()) -> Tuple[Tuple[Any, ...], ...]:
        """执行查询并返回结果"""
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
            return results

    @staticmethod
    def execute(connection, sql: str, params: Tuple = ()) -> None:
        """执行更新、删除等操作"""
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            connection.commit()
