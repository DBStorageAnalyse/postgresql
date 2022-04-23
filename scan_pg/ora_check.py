# -*- coding: utf-8 -*-
# oracle page_check
import struct

s = struct.unpack  # B,H,I,Q


# 记录片段信息：片段号，片段中页数，起始页信息(物理偏移，页号，文件号)，结束页信息(物理偏移，页号，文件号)
class values1:
    def __init__(self):
        self.offset = 0
        self.page_no = 0
        self.file_no = 0
        self.scn = 0


# 记录块信息：块号，块中片段数，起始片段信息(片段编号,起始页号,文件号)，结束片段信息
class values2:
    def __init__(self):
        self.page_sum = 1
        self.file_no_1 = 0
        self.offset_1 = 0
        self.page_no_1 = 0
        self.scn_1 = 0
        self.file_no_2 = 0
        self.offset_2 = 0
        self.page_no_2 = 0
        self.scn_2 = 0
        self.scn_abs = 0
        self.no = 0


class File_Info():
    def __init__(self):
        self.endian = 0  # 平台软件的字节顺序(大小端),0:小端,1:大端
        self.hard = 0  # 用于标识的magic数,4k:0x82,8k:0xa2,16k:0xc2
        self.f_name = ''
        self.f_no = 0  # 文件号
        self.f_type = ''  # 文件类型
        self.blk_size = 0  # 文件的块/页 大小 (B)
        self.blk_sum = 0  # 文件的块/页 总数
        self.real_sum = 0  # 文件真实的块/页 总数
        self.DBID = 0
        self.version = ''
        self.SID = ''
        self.ts_id = 0
        self.ts_name = ''
        self.big_ts = 0  # 是否是大文件
        self.file_create_scn = 0
        self.chkpoint_scn = 0
        self.info = ''
        self.err_pages = []  # 损坏的块


# 　解析文件头(page_1),获取文件信息
def file_blk_1(data):  # blcock#1
    file_info = File_Info();
    fmt_1 = '';
    version = ''
    if data[54:55] == b'\x00' and data[24:25] != b'\x00':
        file_info.endian = 1;
        fmt_1 = '>'
    elif data[54:55] != b'\x00' and data[24:25] == b'\x00':
        file_info.endian = 0;
        fmt_1 = '<'
    file_type = {3: 'dbf', 1: 'ctl', 6: 'temp.dbf', 0: 'unknown'}
    data0 = str(data[32:40], encoding="ascii").strip()  # SID
    data1 = s(fmt_1 + "IQ2H2I2H", data[28:56])
    data2 = s(fmt_1 + "2IH", data[96:106])
    data3 = s(fmt_1 + "IH", data[332:338])
    data4 = str(data[338:338 + data3[1]], encoding="ascii").strip()
    data5 = s(fmt_1 + "IH", data[484:490])
    ver_1 = s("<4B", data[24:28])
    file_create_scn = data2[1] + data2[2] * 4294967296
    chkpoint_scn = data5[0] + data5[1] * 4294967296
    pagefile = data2[0]  # bootstrap$的起始指针
    file = pagefile >> 22;
    page = pagefile - 4194303 * file - file  # 页号
    data_1 = s(fmt_1 + "I", data[4:8]);
    file_1 = data_1[0] >> 22
    if file_1 == 0:
        big_ts = 1
    else:
        big_ts = 0
    type1 = data1[7]
    if fmt_1 == '>':
        version = str(ver_1[0]) + '.' + str(ver_1[1] // 16) + '.' + str(ver_1[1] % 16) + '.' + str(
            ver_1[2]) + '.' + str(ver_1[3])
    elif fmt_1 == '<':
        version = str(ver_1[3]) + '.' + str(ver_1[2] // 16) + '.' + str(ver_1[2] % 16) + '.' + str(
            ver_1[1]) + '.' + str(ver_1[0])
    if type1 not in (1, 3, 6):
        type1 = 0
    data6 = s("B", data[0:1])
    if data6[0] == 21:
        type1 = 1
    file_info.version = version
    file_info.blk_sum = data1[4]
    file_info.blk_size = data1[5]
    file_info.f_no = data1[6]
    file_info.f_type = file_type[type1]
    file_info.big_ts = big_ts
    file_info.SID = data0
    file_info.DBID = data1[0]
    file_info.ts_id = data3[0]
    file_info.ts_name = data4
    file_info.file_create_scn = file_create_scn
    file_info.chkpoint_scn = chkpoint_scn

    s1 = ("DBID:%d,SID:%s,V:%s\nts_id:%d,ts_name:%s,boot:%d,page_size:%dB,big_ts:%d\n" % (
    data1[0], data0, version, data3[0], data4, page, data1[5], big_ts))
    s2 = ("file_no:%d,file_type:%s,page_sum:%d,file_size:%6.3fG\n" % (
    data1[6], file_type[type1], data1[4], data1[4] * data1[5] / 1024 / 1024 / 1024))
    s3 = ("file_create_scn:%d, chkpoint_scn:%d \n" % (file_create_scn, chkpoint_scn))
    file_info.info = s1 + s2 + s3
    # print(file_info.info)
    return file_info, page


# 页面异或校验
def ora_page_check_1(data, fmt_1):
    return 1


# 页头特征值校验, 要尽可能的详细准确
def ora_page_check_2(data, fmt_1):
    fmt = fmt_1 + 'II6HI'  # 大小端
    len_1 = len(data)
    if len(data) != 8192:
        return 0
    data = s(fmt, data[0:24])  # 页头

    if data[0] == 0 and data[1] != 0 and data[2] == 0 and data[3] in (0, 4, 5) and data[4] >= 80 and data[5] < 8190 and \
            data[6] == 8192 and data[7] == 8196 and data[8] == 0:
        return 1
    else:
        return 0
    #  data[3]是是flag; data[2]/data[8]是0000;


# 页尾校验，和页头的值匹配
def ora_page_check_3(data, fmt_1):
    return 1
