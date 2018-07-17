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


logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(funcName)s %(message)s",
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


def getSocketBaseData(gpsFile, vehicleNo="粤BD98553"):
    df_csv = pd.read_csv(gpsFile)
    df_csv.driverStatus.replace(["AVL", "HIRED"], [1, 513], inplace=True)
    n = df_csv["lat"].count()
    if n == 0:
        return
    updatetime = getTimeList(n)
    distance = getDistanceList(n)
    df = pd.DataFrame()
    df["vehiclePlateNumber"] = [vehicleNo] * n
    df["vehiclePlateColorCode"] = [1] * n
    df["encrypt"] = [0] * n
    df["alarm"] = [0] * n
    df["state"] = df_csv["driverStatus"]
    df["lat"] = df_csv["lat"]
    df["lng"] = df_csv["lng"]
    df["vec"] = [3600] * n
    df["direction"] = [179] * n
    df["datetime"] = updatetime
    df["distance"] = distance
    return df


def get_gps_and_convert_and_upload(date: int, driver_id: int, vehicleNo: str):
    date = date
    driver_id = driver_id
    vehicleNo = vehicleNo

    logging.info("开始获取gps数据...")
    get_driver_gps_by_ids(date, [driver_id])
    logging.info("获取gps数据完成...")
    logging.info("开始转换数据...")
    df = getSocketBaseData("{}_{}.csv".format(date, driver_id), vehicleNo=vehicleNo)
    col = ["vehiclePlateNumber", "vehiclePlateColorCode", "encrypt", "alarm", "state", "lat", "lng", "vec", "direction",
           "datetime", "distance"]
    df.to_csv("A00001.csv", header=False, index=False, columns=col, encoding="utf8")
    logging.info("数据转换完成...")
    logging.info("开始登录并上传socket数据...")
    login()
    upload_gps_3_sections()
    logging.info("上传socket数据完成...")
    logging.info("等待300s...")
    sleep(5)
    logging.info("等待结束,退出程序...")


if __name__ == '__main__':
    get_gps_and_convert_and_upload(20180712, -606168582, "粤BD02761")
