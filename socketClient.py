import socket, csv, time
from subPlatform import MsgHeader, CodeConst, LoginBody, getCrcCode, UploadGps, transfer, MsgRealRun
import random, logging

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(funcName)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="run.log",
                    filemode="a")

# HOST, PORT = "127.0.0.1", 61024

# HOST, PORT = "125.70.211.26", 61024

# dev
HOST, PORT = "116.62.129.43", 61024

# 林勇本地环境
# HOST, PORT = "10.1.9.110", 61024
# HOST, PORT = "10.1.11.116", 61024

# 文勇本地环境
# HOST, PORT = "10.1.10.54", 61024

# PROD环境
# HOST, PORT = "120.77.74.12", 61024

# UAT环境
# HOST, PORT = "116.62.242.157", 61024

# 新建全局的socket连接
logging.info("{}:{}".format(HOST, PORT))
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))
sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


def login():
    # 登录8193 & 注销8195
    msg_header = MsgHeader(69, 0, 8193, 0, 1, 2, 15, 0, 0)
    login_body = LoginBody(1234, 1024)
    crc_code = getCrcCode(msg_header.msgHeader + login_body.body)
    all_data = CodeConst.headFlag + transfer(msg_header.msgHeader + login_body.body + crc_code) + CodeConst.endFlag
    sock.send(all_data)
    logging.info(">>{}".format(all_data.hex()))
    data = sock.recv(1024)
    logging.debug("<<{}".format(data.hex()))


def upload_gps_serial():
    # 读取csv文件信息，发送GPS数据
    n = 0
    with open(r"A00001.csv") as csvfile:
        read_csv = csv.reader(csvfile, delimiter=',')
        for row in read_csv:
            upload_gps = UploadGps(row)
            n += 1
            msg_header = MsgHeader(75, n, 8448, 0, 0, 0, 1, 0, 0)
            crc_data = msg_header.msgHeader + upload_gps.uploadGpsBody
            crc_code = getCrcCode(crc_data)
            all_data = CodeConst.headFlag + transfer(
                msg_header.msgHeader + upload_gps.uploadGpsBody + crc_code) + CodeConst.endFlag
            sock.send(all_data)
            logging.info(">>{}".format(all_data.hex()))
            data = sock.recv(1024)
            logging.debug("<<{}-->{}".format(n, data.hex()))


def upload_gps_3_sections():
    # 读取csv文件信息，发送GPS数据
    with open(r"A00001.csv", encoding="utf8") as csvfile:
        send_data = b""
        read_csv = csv.reader(csvfile, delimiter=',')
        n = random.randint(1, 99)
        m = 0
        for row in read_csv:
            msg_header = MsgHeader(75, n * 10000 + m, 8448, 0, 0, 0, 1, 0, 0)
            m += 1
            upload_gps = UploadGps(row)
            crc_data = msg_header.msgHeader + upload_gps.uploadGpsBody
            crc_code = getCrcCode(crc_data)
            all_data = CodeConst.headFlag + transfer(
                msg_header.msgHeader + upload_gps.uploadGpsBody + crc_code) + CodeConst.endFlag
            send_data += all_data
        send_data_len = len(send_data)
        len1 = random.randint(0, int(send_data_len / 2))
        len2 = random.randint(int(send_data_len / 2), send_data_len)
        sock.send(send_data[:len1])
        logging.debug(">>{}".format(send_data[:len1].hex()))
        # data = sock.recv(1024)
        # print(data.hex())
        sock.send(send_data[len1:len2])
        logging.debug(">>{}".format(send_data[len1:len2].hex()))
        # data = sock.recv(1024)
        # print(data.hex())
        sock.send(send_data[len2:])
        logging.debug(">>{}".format(send_data[len2:].hex()))
        data = sock.recv(1024)
        sock.shutdown(2)
        sock.close()
        logging.info("<<{}".format(data.hex()))


def upload_gps_random_sections(filename):
    # 读取csv文件信息，发送GPS数据
    x = random.randint(0, 99)
    y = 0
    with open(filename) as csvfile:
        send_data = b""
        read_csv = csv.reader(csvfile, delimiter=',')
        for row in read_csv:
            msg_header = MsgHeader(75, x * 10000 + y, 8448, 0, 0, 0, 1, 0, 0)
            y += 1
            upload_gps = UploadGps(row)
            crc_data = msg_header.msgHeader + upload_gps.uploadGpsBody
            crc_code = getCrcCode(crc_data)
            all_data = CodeConst.headFlag + transfer(
                msg_header.msgHeader + upload_gps.uploadGpsBody + crc_code) + CodeConst.endFlag

            logging.debug("{}".format(all_data.hex()))
            # print(all_data.hex())
            send_data += all_data
        send_data_len = len(send_data)
        logging.debug(send_data.hex())

        n = random.randint(3, 99)

        parts = []
        for i in range(n):
            parts.append(random.randint(1, send_data_len))

        parts.sort()
        logging.debug(parts)
        logging.debug(len(parts))

        for n in range(len(parts)):
            if 0 == n:
                sock.send(send_data[:parts[n]])
                logging.info("BEGIN [{}]{}-->{} ".format(parts[n], n, send_data[:parts[n]].hex()))
            elif n == len(parts) - 1:
                sock.send(send_data[parts[n - 1]:])
                logging.info("END [{}]{}-->{}".format(parts[n - 1], n, send_data[parts[n - 1]:].hex(), parts[n]))
            else:
                sock.send(send_data[parts[n - 1]:parts[n]])
                logging.debug(
                    "MIDDLE [{}>>{}]{}-->{}".format(parts[n - 1], parts[n], n, send_data[parts[n - 1]:parts[n]].hex()))

        data = sock.recv(1024)
        logging.info("<<{}".format(data.hex()))


def upload_gps4(vehicle_no):
    # 读取csv文件信息，发送GPS数据
    x = random.randint(0, 99)
    y = 0
    with open(r"A00001.csv") as csvfile:
        send_data = b""
        read_csv = csv.reader(csvfile, delimiter=',')
        for row in read_csv:
            row[0] = vehicle_no
            msg_header = MsgHeader(75, x * 10000 + y, 8448, 0, 0, 0, 1, 0, 0)
            y += 1
            upload_gps = UploadGps(row)
            crc_data = msg_header.msgHeader + upload_gps.uploadGpsBody
            crc_code = getCrcCode(crc_data)
            all_data = CodeConst.headFlag + transfer(
                msg_header.msgHeader + upload_gps.uploadGpsBody + crc_code) + CodeConst.endFlag
            send_data += all_data
        send_data_len = len(send_data)

        n = random.randint(3, 99)

        parts = []
        for i in range(n):
            parts.append(random.randint(1, send_data_len))

        parts.sort()
        logging.debug(parts)
        logging.debug(len(parts))

        for n in range(len(parts)):
            if 0 == n:
                sock.send(send_data[:parts[n]])
                logging.debug("BEGIN [{}]{}-->{} ".format(parts[n], n, send_data[:parts[n]].hex()))
            elif n == len(parts) - 1:
                sock.send(send_data[parts[n]:])
                logging.debug("END [{}]{}-->{}".format(parts[n], n, send_data[parts[n]:].hex(), parts[n]))
            else:
                sock.send(send_data[parts[n - 1]:parts[n]])
                logging.debug("MIDDLE [{}]{}-->{}".format(parts[n], n, send_data[parts[n - 1]:parts[n]].hex()))

        data = sock.recv(1024)
        logging.debug("<<{}".format(data.hex()))


def upload_msg_real_run():
    # 读取csv文件信息，发送运营数据
    msg_header = MsgHeader(108, 0, 8448, 0, 0, 0, 1, 0, 0)
    n = 0
    with open(r"B00001.csv") as csvfile:
        read_csv = csv.reader(csvfile, delimiter=',')
        for row in read_csv:
            msg_real_run = MsgRealRun(row)
            n += 1

            crc_data = msg_header.msgHeader + msg_real_run.msgRealRunBody
            crc_code = getCrcCode(crc_data)
            all_data = CodeConst.headFlag + transfer(
                msg_header.msgHeader + msg_real_run.msgRealRunBody + crc_code) + CodeConst.endFlag
            sock.send(all_data)
            data = sock.recv(1024)
            logging.debug(">>{}-->{}".format(n, data.hex()))


if __name__ == '__main__':
    login()
    upload_gps_3_sections()
    # upload_gps_random_sections("vehicle.csv")
    while True:
        time.sleep(300)
