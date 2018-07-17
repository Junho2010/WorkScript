from pymongo import MongoClient
from pandas import DataFrame
import datetime, time
import pandas as pd
import numpy as np

import logging

logging.basicConfig()

logging.basicConfig(level=print,
                    format="%(asctime)s %(levelname)s %(funcName)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="run.log",
                    filemode="a")


def getTimeList(endtime, n):
    d = pd.date_range(end=endtime, periods=n, freq="2s")
    d = d.to_pydatetime()
    d = np.vectorize(lambda s: s.strftime('%y%m%d%H%M%S'))(d)
    return d


def toTime(timestamp):
    timeArray = time.localtime(timestamp)
    return time.strftime("%y%m%d%H%M%S", timeArray)


class MongoMe(object):
    def __init__(self, host, port, username, passwd, database="mobility_exchange",
                 collection="meMongoDriverTripGpsHist_20180712"):
        """
        根据输入的Mongo连接信息，返回一个mongod的集合
        :param host: 域名或IP
        :param port: 端口
        :param username: 用户名
        :param passwd: 密码
        :param database: 数据库名称, 默认值mobility_exchange
        :param collection: 集合名称， 默认值meMongoDriverTripGpsHist
        """
        self.connection = MongoClient(host, port)
        self.database = self.connection.get_database(database)
        self.database.authenticate(name=username, password=passwd)
        self.collection_gps = self.database.get_collection(collection)
        logging.debug("登录：{}_{}_{}_{}".format(host, port, database, collection))

        self.driver_ids = []
        self.gps = []

    def get_driver_ids(self, date, num=50):
        """
        获取指定日期前num条数据中，不重复的司机列表
        :param num: 查询的数据条数，默认查询50条数据
        :param date:日期，数字型，20180531
        :return:返回司机ID列表
        """
        query = {
            "logDate": date,
            "driverStatus": "HIRED",
            "driverId": {"$gt": 0}
        }
        cursor = self.collection_gps.find(query).limit(num)
        for result in cursor:
            self.driver_ids.append(result["driverId"])
            logging.debug("获取到司机ID:{}".format(result["driverId"]))

        self.driver_ids = list(set(self.driver_ids))
        print("所有司机ID列表:{}".format(self.driver_ids))

    def get_driver_gps(self, driver_id: int, date: int = None):
        """
        根据司机ID获取司机某天的所有GPS轨迹，默认获取昨天的
        :param driver_id:
        :param date:
        :return: 司机一天的GPS列表，每一个元素为一个GPS点
        """
        pipeline = [{"$match": {"driverId": driver_id, "logDate": date}},
                    {"$project": {"driverId": 1, "gpsHistList": 1, "updateTime": 1, "driverStatus": 1, "vehicleNo": 1}},
                    {"$sort": {"updOn": 1}}]

        print("查询司机GPS条件:日期_{},司机ID_{}".format(date, driver_id))

        cursor = self.collection_gps.aggregate(pipeline=pipeline, allowDiskUse=True)
        for result in cursor:
            print(1)
            data = result["gpsHistList"]
            # updatetime = result["updateTime"]
            # time_list = getTimeList(updatetime, len(data))
            # n = 0
            for d in data:
                d["driverStatus"] = result["driverStatus"]
                d["updateTime"] = toTime(int(d["updateTime"]) / 1000)
                d["vehicleNo"] = result["vehicleNo"]
                self.gps.extend(data)
            print("新增GPS数据:{}".format(data))

        print("获取司机_{}的GPS数据完成".format(driver_id))


# def get_driver_gps(date, num=5000, driver_num=30, drop_duplicates=False):
#     """
#     获取指定司机数量（最多，实际可能会少一些）的GPS数据
#     :param drop_duplicates: 是否对结果中的GPSs数据进行去重操作，默认不去重
#     :param driver_num: 从mongoDB中拿去的数据条数
#     :param date: 获取数据的日期
#     :param num: 获取的GPS原始数据条数，一条可能有多个GPS点
#     :return: 无返回结果，会将结果写入当前目录下的driverId_date.csv文件中
#     """
#     mongo_gps = MongoMe("119.23.8.231", 27128, "meuser", "NLb6ceTnOKAILorG8VVdY")
#     # mongo_gps.get_driver_ids(date, driver_num)
#     mongo_gps.driver_ids = ["-1205968538"]
#     for driver_id in mongo_gps.driver_ids:
#         mongo_gps.get_driver_gps(driver_id, date)
#         print("开始处理司机[{}]在[{}]的GPS数据".format(driver_id, date))
#         df = DataFrame(mongo_gps.gps)
#         df.sort_values(by=["updateTime"], inplace=True)
#         df = df[["lat", "lng", "driverStatus"]]
#         if drop_duplicates:
#             df = df.drop_duplicates()
#             print("GPS数据已经去重")
#         df.to_csv("{}_{}.csv".format(date, driver_id), header=False, index=False)
#         print("数据已经存储在文件{}_{}.csv中".format(date, driver_id))


def get_driver_gps_by_ids(date, driver_ids, drop_duplicates=False):
    """
    获取指定司机数量（最多，实际可能会少一些）的GPS数据
    :param driver_ids: 司机ID列表
    :param drop_duplicates: 是否对结果中的GPSs数据进行去重操作，默认不去重
    :param driver_num: 从mongoDB中拿去的数据条数
    :param date: 获取数据的日期
    :param num: 获取的GPS原始数据条数，一条可能有多个GPS点
    :return: 无返回结果，会将结果写入当前目录下的driverId_date.csv文件中
    """
    mongo_gps = MongoMe("dds-wz922d6f2750cde41228-pub.mongodb.rds.aliyuncs.com", 3717, "meuser", "NLb6ceTnOKAILorG8VVdY")
    print("登录成功...")
    mongo_gps.driver_ids = driver_ids
    for driver_id in mongo_gps.driver_ids:
        print("开始...")
        mongo_gps.get_driver_gps(driver_id, date)
        print("开始处理司机[{}]在[{}]的GPS数据".format(driver_id, date))
        df = DataFrame(mongo_gps.gps)
        # df.sort_values(by=["updatetime"], inplace=True)
        # df = df[["lat", "lng", "driverStatus", "updatetime"]]
        if drop_duplicates:
            df = df.drop_duplicates()
            print("GPS数据已经去重")
        df.to_csv("{}_{}.csv".format(date, driver_id), header=True, index=False)
        print("数据已经存储在文件{}_{}.csv中".format(date, abs(driver_id)))


if __name__ == '__main__':
    get_driver_gps_by_ids(20180716, [-608525505], False)
