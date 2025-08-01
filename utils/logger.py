import logging
import os

# 创建日志目录，如果目录不存在，则创建它
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 创建不同模块的 Logger
def create_logger(module_name: str) -> logging.Logger:
    # 创建一个 logger 对象
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.ERROR)  # 设置日志级别为 ERROR

    # 创建一个 Handler 用于写入文件
    log_file = os.path.join(log_dir, f"{module_name}_failure.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.ERROR)  # 设置 Handler 级别为 ERROR

    # 创建一个 Formatter，用于格式化日志
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    # 给 logger 添加 Handler
    logger.addHandler(file_handler)

    return logger