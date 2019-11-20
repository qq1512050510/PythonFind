# -*- coding: utf-8 -*-
#!/usr/bin/python3
# @Time    : 2019/9/10 11:21
# @Author  : bjsasc
import json
import logging
import os
import sys
import time
import DataUtil
from logging.handlers import TimedRotatingFileHandler
from pyinotify import WatchManager, Notifier, ProcessEvent, EventsCodes
import threading


def heartBeat(log):
    log.info("beat 时间：" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def timeTaks(time: int, time_event, log):
    time_event(log)
    global timer
    timer = threading.Timer(time, timeTaks, [time, time_event, log])
    timer.start()


# 生成log文件
def init_log():
    
    log = logging.getLogger("simple_exmaple")
    log.setLevel(logging.INFO)
    # 如果有日志的话 每天记录 最长备份周期为365天
    ch = TimedRotatingFileHandler("./log/FileWatch.log", when="D", backupCount=365, encoding="utf-8")
    ch.setLevel(logging.DEBUG)
    fs = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    log.addHandler(ch)
    log.addHandler(fs)
    
    # log.debug("debug message")
    log.info("log init 时间：%s", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # log.warning("warn message")
    # log.error("error message")
    # log.critical("critical message")
    return log;


log = init_log()
# 定时启动任务
# timeTaks(6,heartBeat)
timeTaks(60, heartBeat, log)
# timeTaks(60*60*24,init_log)
# timer = threading.Timer(1,heartBeat)
# timer.start()
# 设置日志输出两个handle，屏幕和文件
'''
log = logging.getLogger('file watch ---')
fp = logging.FileHandler('a.log', 'a+', encoding='utf-8')
fs = logging.StreamHandler()
log.addHandler(fs)
log.addHandler(fp)
log.setLevel(logging.DEBUG)
'''
FILE_DIR = r'/home/bjsasc/test/'  # 监听文件目录


def check_dir_exist():
    """
        检查文件目录是否存在
    """
    if not FILE_DIR:
        log.info("The WATCH_PATH setting MUST be set.")
        sys.exit()
    else:
        if os.path.exists(FILE_DIR):
            log.info('Found watch path: path=%s.' % (FILE_DIR))
        else:
            log.info('The watch path NOT exists, watching stop now: path=%s.' % (FILE_DIR))
            # os.mkdir(FILE_DIR)
            sys.exit()


def read_json_from_file(file_path):
    """
    从文件中读取json数据
    :param file_path:
    """
    try:
        with open(file_path) as f:
            s = f.read()
            log.info("读取文件的内容为：%s", s)
            try:
                print(s.strip() != "")
                if s.strip() != "":
                    log.info("执行Json parse")
                    result = json.loads(s)
                    log.info("josn loads的结果：%s", result);
                    # 处理数据
                    for i in result["product_list"]["List"]:
                        data_process(i)
            except Exception as e:
                log.error("异常,提示信息：%s", e)
            finally:
                log.info("finally...")
                # pass
    except Exception as e:
        log.error("open filf异常，提示信息：%s", e);


def data_process(data: dict):
    """
    处理从json中读取到的数据
    :param data:
    """
    file_path = data["Directory"]
    name = data["Product"]
    archiveDir = DataUtil.get_archieve_dir()[0]["archiveDir"]
    log.info("archieveDir: %s", archiveDir)
    archieveFile = archiveDir+"/"+name
    # 从文件名称获取文件信息
    name_info = DataUtil.parse_name(file_path)
    
    # 获取卫星和载荷信息
    #if name_info.strip() != "" and len(name_info) >= 2:
    #    weixing_info = name_info[0]
    #    zaihe_info = name_info[1]
    # 打开文件检查
    checknum = DataUtil.check_file(file_path)
    # 拷贝文件
    DataUtil.copy_file(file_path, archieveFile)
    
    # 构造保存数据库的dict
    result = {}
    result['type'] = '1'
    result['name'] = name
    result['suffix'] = 'fits'
    result['sourcepath'] = file_path
    result['checknum'] = checknum
    result['archivepath'] = archieveFile
    result['status'] = '1'
    # 保存数据到数据库
    DataUtil.save_data(result)
    
    # 更新数据
    DataUtil.update_data()
    # 调用远程接口
    # DataUtil.notice(file_path)


class EventHandler(ProcessEvent):

    def process_IN_CLOSE_WRITE(self, event):
        """
        监听文件传输完成时间，只实现了传输完成监听
        :param event:
        """
        log.info("发现文件：%s", event.name);
        log.info("文件全路经: %s " % os.path.join(event.path, event.name))
        file_path = os.path.join(event.path, event.name)
        time.sleep(2)
        log.info('开始解析文件：%s' % (file_path))
        read_json_from_file(file_path)
        

def main():
    """
    文件监听的入口程序
    """
    check_dir_exist()
    wm = WatchManager()
    notifier = Notifier(wm, EventHandler())
    wm.add_watch(FILE_DIR, EventsCodes.FLAG_COLLECTIONS.get("OP_FLAGS").get("IN_CLOSE_WRITE"), rec=True, auto_add=True)
    log.info('Now starting monitor ：%s' % (FILE_DIR))
    notifier.loop()


if __name__ == '__main__':
    main()
