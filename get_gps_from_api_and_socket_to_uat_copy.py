"""
__author__ : "Will He"
Create Date : 2018/7/6 9:40
"""
import numpy as np
import pandas as pd
import datetime
import logging
from time import sleep
from mongoME import get_driver_gps_by_ids
from socketClient import upload_gps_3_sections, login

from pymongo import MongoClient
from pandas import DataFrame

logging.basicConfig()
logging.basicConfig(level=print,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="allRun.log",
                    filemode="a")


# 获取时间序列
def getTimeList(n):
    d = pd.date_range(end=datetime.datetime.now(), periods=n, freq="2s")
    d = d.to_pydatetime()
    d = np.vectorize(lambda s: s.strftime('%y%m%d%H%M%S'))(d)
    return d


# 获取距离列表
def getDistanceList(n, begin=10000):
    distanceList = []
    for i in range(n):
        distanceList.append(begin + i * 2)
    return distanceList


def getSocketBaseData(gpsFile: str, vehicleNo=""):
    """
    将查询出的gps现网数据转换为socket传输的原始数据文件格式
    :param gpsFile: gpsFile源文件
    :param vehicleNo: 长度小于5时，gpsFile中的车牌号，否则使用设置的车牌号
    :return: 用于socket数据传输的二维表
    """
    df_csv = pd.read_csv(gpsFile)
    df_csv.driverStatus.replace(["AVL", "HIRED"], [1, 513], inplace=True)
    n = df_csv["lat"].count()
    if n == 0:
        return
    # updatetime = getTimeList(n)
    distance = getDistanceList(n)
    df = pd.DataFrame()
    if len(vehicleNo) <= 6:
        df["vehiclePlateNumber"] = df_csv["vehicleNo"]
    else:
        df["vehiclePlateNumber"] = [vehicleNo] * n
    df["vehiclePlateColorCode"] = [1] * n
    df["encrypt"] = [0] * n
    df["alarm"] = [0] * n
    df["state"] = df_csv["driverStatus"]
    df["lat"] = df_csv["lat"]
    df["lng"] = df_csv["lng"]
    df["vec"] = [3600] * n
    df["direction"] = [179] * n
    df["datetime"] = df_csv["updatetime"]
    df["distance"] = distance
    return df


def get_gps_and_convert_and_upload(date: int, driver_id: int, vehicleNo: str):
    date = date
    driver_id = driver_id
    vehicleNo = vehicleNo

    print("开始获取gps数据...")
    get_driver_gps_by_ids(date, [driver_id])
    print("获取gps数据完成...")
    print("开始转换数据...")
    df = getSocketBaseData("{}_{}.csv".format(date, driver_id), vehicleNo=vehicleNo)
    col = ["vehiclePlateNumber", "vehiclePlateColorCode", "encrypt", "alarm", "state", "lat", "lng", "vec", "direction",
           "datetime", "distance"]
    df.to_csv("A00001.csv", header=False, index=False, columns=col, encoding="utf8")
    print("数据转换完成...")
    print("开始登录并上传socket数据...")
    login()
    upload_gps_3_sections()
    print("上传socket数据完成...")
    sleep(5)


# 连接数据库, 获取数据
def getData():
    user = "meuser"
    pwd = "NLb6ceTnOKAILorG8VVdY"
    server_logs = "119.23.8.231"
    port = "27128"
    db_name = "mobility_exchange"

    uri = 'mongodb://' + user + ':' + pwd + '@' + server_logs + ':' + port
    client = MongoClient(uri)
    db = client[db_name]

    pipeline = [{"$match": {"hubCode": "SZ_AIR_BA", "event": "METER_ON"}},
                {"$project": {"driverId": 1, "vehicleNo": 1}}]

    data = db["meMogTransportHubEvent"].aggregate(pipeline, allowDiskUse=True)
    print("查询完成")
    return data


def dealData(data):
    l = []
    for c in data:
        l.append(c)

    df = DataFrame(l)
    del df["_id"]
    df.drop_duplicates(inplace=True)
    return df


if __name__ == '__main__':
    data = getData()
    df = dealData(data)
    for index, row in df.iterrows():
        get_gps_and_convert_and_upload(20180712, int(row["driverId"]), row["vehicleNo"])

