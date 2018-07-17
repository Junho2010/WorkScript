import struct
import datetime
import hashlib

import crcmod.predefined
from binascii import unhexlify


def transfer(str):
    if type(str) == bytes:
        data = str
    else:
        data = bytes.fromhex(str)

    data = data.replace(bytes.fromhex("5a"), bytes.fromhex("5a02"))
    data = data.replace(bytes.fromhex("5b"), bytes.fromhex("5a01"))
    data = data.replace(bytes.fromhex("5e"), bytes.fromhex("5e02"))
    data = data.replace(bytes.fromhex("5d"), bytes.fromhex("5e01"))

    return data


def getCrcCode(data):
    s = data
    if type(s) is not bytes:
        s = unhexlify(s)
    crc16 = crcmod.predefined.Crc('ccitt-false')
    crc16.update(s)
    return bytes.fromhex(crc16.hexdigest())
    # return crc16.hexdigest().encode("gbk")


def getMD5(src):
    m = hashlib.md5()
    m.update(src.encode('UTF-8'))
    return m.hexdigest()


class MsgHeader(object):
    def __init__(self, msgLength, msgSn, msgId, msgGnsscenterId, versionFlag1, versionFlag2, versionFlag3, encryptFlag,
                 encryptKey):
        self.msgLength = msgLength
        self.msgSn = msgSn
        self.msgId = msgId
        self.msgGnsscenterId = msgGnsscenterId
        self.versionFlag1 = versionFlag1
        self.versionFlag2 = versionFlag2
        self.versionFlag3 = versionFlag3
        self.encryptFlag = encryptFlag
        self.encryptKey = encryptKey
        # self.head_before = struct.pack(">2I", self.msgLength, self.msgSn)
        # self.head_middle = bytes.fromhex("2001")
        # self.head_end = struct.pack(">I4BI")

        self.msgHeader = struct.pack(">2IHI4BI",
                                     self.msgLength,
                                     self.msgSn,
                                     self.msgId,
                                     self.msgGnsscenterId,
                                     self.versionFlag1,
                                     self.versionFlag2,
                                     self.versionFlag3,
                                     self.encryptFlag,
                                     self.encryptKey)


class CodeConst(object):
    headFlag = b"["
    endFlag = b"]"


class LoginBody(object):
    def __init__(self, userID, password):
        self.userID = struct.pack(">I", userID)
        self.password = password
        self.connectTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.connectTime_bcd = bytes.fromhex(self.connectTime)
        self.mac = getMD5(str(self.password) + self.connectTime)
        self.mac = self.mac.encode("gbk")
        self.body = self.userID + self.connectTime_bcd + self.mac


class UploadGps(object):
    def __init__(self, dataList):
        self.dataType = struct.pack(">H", 8449)
        self.dataLength = struct.pack(">I", 43)
        self.vehiclePlateNumber = struct.pack(">12s", dataList[0].encode("gbk"))
        self.vehiclePlateColorCode = struct.pack(">B", int(dataList[1]))
        self.encrypt = struct.pack(">B", int(dataList[2]))
        self.alarm = struct.pack(">I", int(dataList[3]))
        self.state = struct.pack(">I", int(dataList[4]))
        self.lat = struct.pack(">I", round((float(dataList[5]) + 0.002667) * 600000))
        self.lon = struct.pack(">I", round((float(dataList[6]) - 0.004846) * 600000))
        self.vec = struct.pack(">H", round(int(dataList[7]) * 10))
        self.direction = struct.pack(">B", int(dataList[8]))
        self.datetimme = bytes.fromhex(dataList[9])
        self.distance = struct.pack(">I", int(dataList[10]))
        self.uploadGpsBody = self.dataType + self.dataLength + self.vehiclePlateNumber + self.vehiclePlateColorCode + \
                             self.encrypt + self.alarm + self.state + self.lat + self.lon + self.vec + \
                             self.direction + self.datetimme + self.distance


class MsgRealRun(object):
    def __init__(self, dataList):
        self.dataType = struct.pack(">H", 8451)
        self.dataLength = struct.pack(">I", 76)
        self.vehiclePlateNumber = struct.pack(">12s", dataList[0].encode("gbk"))
        self.vehiclePlateColorCode = struct.pack(">B", int(dataList[1]))
        self.encrypt = struct.pack(">B", int(dataList[2]))
        self.lat_on = struct.pack(">I", round(float(dataList[3]) * 100000))
        self.lon_on = struct.pack(">I", round(float(dataList[4]) * 100000))
        self.time_get_on = bytes.fromhex(dataList[5])
        self.lat_off = struct.pack(">I", round(float(dataList[6]) * 100000))
        self.lon_off = struct.pack(">I", round(float(dataList[7]) * 100000))
        self.time_get_off = bytes.fromhex(dataList[8])
        self.employee_id = struct.pack(">18s", dataList[9].encode("gbk"))
        self.service_eval_idx = struct.pack(">B", int(dataList[10]))
        self.service_eval_idx_ext = struct.pack(">H", int(dataList[11]))
        self.run_odometer = struct.pack(">H", int(dataList[12]))
        self.empty_odometer = struct.pack(">H", int(dataList[13]))
        self.fuel_surcharge = struct.pack(">H", int(dataList[14]))
        self.time_wait = bytes.fromhex(dataList[15])
        self.income = struct.pack(">H", int(dataList[16]))
        self.in_flag = struct.pack(">B", int(dataList[17]))

        self.msgRealRunBody = self.dataType + self.dataLength + self.vehiclePlateNumber + self.vehiclePlateColorCode + \
                              self.encrypt + self.lat_on + self.lon_on + self.time_get_on + self.lat_off + self.lon_off + \
                              self.time_get_off + self.employee_id + self.service_eval_idx + self.service_eval_idx_ext + \
                              self.run_odometer + self.empty_odometer + self.fuel_surcharge + self.time_wait + \
                              self.income + self.in_flag
