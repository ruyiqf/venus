#coding:GBK
import os
import sys
import logging

class DefaultLogHandler(object):
    """默认Log类基础系统支持"""
    def __init__(self, name='mylog', filepath='default.log', log_type='stdout', log_level='DEBUG'):
        self.logger = logging.getLogger(name)
        self.loglevel = {'CRITICAL': logging.CRITICAL,
                         'ERROR': logging.ERROR,
                         'WARNING': logging.WARNING,
                         'INFO': logging.INFO,
                         'DEBUG': logging.DEBUG}

        self.logger.setLevel(self.loglevel[log_level])
        fmt = logging.Formatter(fmt='%(asctime)s: %(levelname)s %(name)s %(message)s')
        if not self.logger.handlers:
            if log_type == 'stdout':
                #创建一个handler，用于标准输出
                ch = logging.StreamHandler()
                ch.setFormatter(fmt)
                self.logger.addHandler(ch)
            if log_type == 'file':
                #创建一个handler，用于输出日志文件
                if os.path.exists(filepath) is False:
                    #Linux创建文件 
                    #os.system('touch ' + filepath)
                    #Windows创建文件
                    os.system('type NUL >' + filepath)
                fh = logging.FileHandler(filepath)
                fh.setFormatter(fmt)
                self.logger.addHandler(fh)
    
    def info(self, *args, **kwargs):
        self.logger.info(*args, **kwargs)

    def info(self, *args, **kwargs):
        self.logger.info(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self.logger.debug(*args, **kwargs)

    def warn(self, *args, **kwargs):
        self.logger.warning(*args, **kwargs)

    def critical(self, *args, **kwargs):
        self.logger.critical(*args, **kwargs)

    def error(self, *args, **kwargs):
        self.logger.error(*args, **kwargs)
