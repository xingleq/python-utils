import pandas as pd
import pymysql

import log

"""
MYSQL 连接
"""


class MysqlConnect:

    # 初始化
    def __init__(self, host, port, user, password, database):
        self._con = pymysql.connect(host=host, port=port, user=user, passwd=password, db=database)
        self._cur = self._con.cursor()
        logger = log.Logger('mysql_connect')
        self._log = logger.get_logger()

    def __del__(self):
        self._cur.close()
        self._con.close()

    # 查询列表
    def query_sql(self, sql):
        result = None
        try:
            self._log.info("QUERY SQL : " + sql)
            self._cur.execute(sql)
            result = self._cur.fetchall()
            return result
        except Exception as e:
            self._log.error("QUERY ERROR : " + str(e))
        return result

    # 查询dataframe格式数据
    def query_sql_dataframe(self, sql):
        result = None
        try:
            self._log.info("query_sql_dataframe : " + sql)
            self._cur.execute(sql)
            data = self._cur.fetchall()
            columns_desc = self._cur.description
            columns = [columns_desc[i][0] for i in range(len(columns_desc))]
            df = pd.DataFrame([list(i) for i in data], columns=columns)
            return df
        except Exception as e:
            self._log.error("query_sql_dataframe ERROR : " + str(e))
        return result

    # 查询数据带列名
    def query_sql_has_col(self, sql):
        result = []
        try:
            self._log.info("QUERY SQL : " + sql)
            self._cur.execute(sql)
            rows = self._cur.fetchall()
            cols = [i[0] for i in self._cur.description]
            for row in rows:
                data = {}
                for i in range(len(cols)):
                    data[cols[i]] = row[i]
                result.append(data)
            return result
        except Exception as e:
            self._log.error("QUERY ERROR : " + str(e))
        return result

    # 查询单条数据
    def query_one(self, sql):
        result = []
        try:
            self._log.info("QUERY SQL : " + sql)
            self._cur.execute(sql)
            result = self._cur.fetchone()
            return result
        except Exception as e:
            self._log.error("QUERY ERROR : " + str(e))
        return result

    # 更新
    def update_sql(self, sql):
        try:
            self._log.info("UPDATE SQL : " + sql)
            self._cur.execute(sql)
            self._con.commit()
            return True
        except Exception as e:
            self._log.error("UPDATE ERROR : " + str(e))
            self._con.rollback()
            return False

    # 获取列名
    def query_cols(self, table):
        try:
            sql = "SELECT * FROM `" + table + "` LIMIT 1"
            self._log.info("GET COLS : " + sql)
            self._cur.execute(sql)
            cols = [i[0] for i in self._cur.description]
            return cols
        except Exception as e:
            self._log.error("GET COLS : " + str(e))
            self._con.rollback()
            return False

    # 批量插入
    def batch_insert(self, table, cols, data):
        try:
            self._log.info("batch_insert : " + table)
            cols_zw = ''
            col = ''
            for c in cols:
                cols_zw = cols_zw + '%s,'
                col = col + '`' + c + '`,'
            sql = "INSERT INTO `" + table + "`(" + col[0:len(col) - 1] + ") VALUES(" + cols_zw[0:len(cols_zw) - 1] + ")"
            self._cur.executemany(sql, data)
            self._con.commit()
        except Exception as e:
            self._log.error("GET COLS : " + str(e))
            self._con.rollback()
            return False
