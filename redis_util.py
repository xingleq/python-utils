import redis

import common

'''
Redis 工具
'''


class RedisHelper:

    # Redis 初始化
    def __init__(self, host, port, database):
        self.pool = redis.ConnectionPool(host=host,
                                         port=port,
                                         db=database)
        self._redis = redis.Redis(connection_pool=self.pool)

    # String 设置值
    def set(self, key, value):
        self._redis.set(key, value)

    # String 获取值
    def get(self, key):
        return self._redis.get(key).decode()

    # 判断key是否存在
    def is_exists(self, key):
        self._redis.exists(key)

    # Hash 设置值
    def hset(self, name, key, value):
        self._redis.hset(name, key, value)

    # Hash 获取值
    def hget(self, name, key):
        return self._redis.hget(name, key).decode()
