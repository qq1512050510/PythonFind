# -*- coding: utf-8 -*-
# @Time    : 2019/10/10 15:28
# @Author  : bjsasc
import pymysql


# 封装mysql的client，支持with用法，让代码简洁一些
class DBClient:

    def __init__(self, host='192.168.43.95', port=3306, db='gns', user='root', passwd='success', charset='utf8'):
        # 建立连接
        self.conn = pymysql.connect(host=host, port=port, db=db, user=user, passwd=passwd, charset=charset)
        # 创建游标，操作设置为字典类型
        self.cur = self.conn.cursor(cursor=pymysql.cursors.DictCursor)

        # 这个是list的游标类型
        # self.cur = self.conn.cursor()
    def __enter__(self):
        # 返回游标
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 提交数据库并执行
        self.conn.commit()
        # 关闭游标
        self.cur.close()
        # 关闭数据库连接
        self.conn.close()
