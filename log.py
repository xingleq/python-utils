import logging
import logging.handlers
import re
from logging.handlers import TimedRotatingFileHandler

'''
日志工具类
'''


class Logger:
    def __init__(self, filename):
        self.log_filename = filename + '.log'
        self._formatter = logging.Formatter('%(asctime)s - %(process)d - %(pathname)s - %(levelname)s:%(message)s')

        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)
        self.set_console_logger()
        self.set_file_logger()

    def set_console_logger(self):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self._formatter)
        self._logger.addHandler(console_handler)

    def set_file_logger(self):
        # interval 滚动周期
        # when='MIDNIGHT', interval=1 表示每天0点为更新点，每天生成一个文件
        # backupCount 表示日志保存个数
        file_handler = TimedRotatingFileHandler(filename=self.log_filename, when='MIDNIGHT', interval=1, backupCount=30,
                                                encoding='utf-8')
        file_handler.suffix = '%Y-%m-%d'
        file_handler.exMatch = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        file_handler.setFormatter(self._formatter)
        self._logger.addHandler(file_handler)

    def get_logger(self):
        return self._logger
