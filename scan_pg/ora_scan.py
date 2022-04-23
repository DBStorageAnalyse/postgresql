#!/usr/bin/env python
#  oracle 碎片扫描. 支持大,小端存储. python3.x。存储信息到sqlite
from PyQt5.QtCore import QThread, pyqtSignal
import struct, sqlite3, time  # ,self.logging
import ora_check

# from pysqlcipher3 import dbapi2 as sqlite3
s = struct.unpack  # B H I Q


# 扫描线程, 需要传参数
class Scan_1(QThread):  # 类Scan_1继承自 QThread
    PUP = pyqtSignal(int)  # 更新进度条的信号
    LUP = pyqtSignal(str)

    def __init__(self, parent=None):  # 解析函数
        super(Scan_1, self).__init__(parent)
        self.f_name = 0
        self.db_name = 0
        self.logging = 0
        self.start_off = 0
        self.end_off = 0
        self.endian = 0
        self.page_size = 0
        self.scan_size = 0
        self.file_infos = []

    def run(self):
        self.ora_scan(self.f_name, self.db_name, self.logging, self.start_off, self.end_off, self.endian,
                      self.page_size, self.scan_size)

    # 扫描页面碎片
    def ora_scan(self, f_name, db_name, logging, start_off, end_off, endian, page_size, scan_size):
        # f_name是要扫描的文件，db_name 输出数据库,start 扫描的起始偏移(字节)，endian 大小端(1:大端)，page_size是oracle页面大小，scan_size是扫描的步长
        # f_name是要扫描的文件，db_name 输出数据库,start 扫描的起始偏移(字节)，endian 大小端(1:大端)，page_size是oracle页面大小，scan_size是扫描的步长
        # print(f_name,db_name,self.start_off,self.end_off,endian,page_size,scan_size)
        begin = time.time()
        print("\nDatetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(begin)) + '\t 扫描开始...')  # current time
        logging.info("\nDatetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(begin)) + '\t 扫描开始...')
        print('f_name:%s\ndb_name:%s\n' % (f_name, db_name))
        logging.info('f_name:%s\ndb_name:%s\n' % (f_name, db_name))
        f = open(f_name, 'rb')  # 以只读方式打开
        f1 = open(db_name, 'wb');
        f1.close()  # 打开sqlite数据库
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()  # 数据库游标
        cursor.execute(
            "create table ora_page(id integer primary key,offset,page_type,obj_id,scn,page_no,file_no,col_sum)")
        cursor.execute(
            "create table link_1(id integer primary key,offset_1,offset_2,scn_1,scn_2,page_sum,page_no_1,page_no_2,file_no_1,g1_id,g1in_id,s_pos,e_pos,blk_sum)")
        cursor.execute("create index page_idx on ora_page(page_no,file_no)")  # 创建索引
        scan_size = scan_size  # 512字节      # 扫描的步长 ,精度, 影响I/O速度
        buff_size = 8 * 1024 * 1024  # baffer大小.  8M
        page_size = page_size  # 8192         # 页面大小.
        start = start_off  # //512*512
        f_size = end_off - start  # 扫描的总大小
        loop_1 = f_size // buff_size
        loop_1_1 = f_size % buff_size
        loop_2 = buff_size // scan_size
        loop_2_1 = loop_1_1 // scan_size
        sum = 0
        if endian == 1:  # 大端
            fmt_1 = '>'
        elif endian == 0:  # 小端
            fmt_1 = '<'

        for i in range(0, loop_1 + 1):  # 处理文件的所有buffer块
            f.seek(i * buff_size + start)
            data = f.read(buff_size + page_size)
            if (i < loop_1):  # 处理文件的所有buffer块
                for ii in range(0, loop_2):
                    pos1 = ii * scan_size
                    offset = i * buff_size + ii * scan_size + start
                    data1 = data[pos1:pos1 + page_size]
                    fmt = fmt_1 + 'II6HI'
                    data2 = s(fmt, data1[0:24])
                    chk2 = ora_check.ora_page_check_2(data1, fmt_1)  # 页头特征值校验
                    # if (i*(buff_size//scan_size)+ii)%16==0:   # 测试 那个页面没过
                    #     print('%d,chk2：%d， flag：%d'%((i*(buff_size//page_size)+ii)//16,chk2,flag))
                    if chk2 == 1:  # 页头校验通过
                        chk3 = ora_check.ora_page_check_3(data1, fmt_1)  # 页尾校验
                        if chk3 == 1:
                            sum += 1
                            ii += page_size // scan_size
                            cursor.execute(
                                "insert into ora_page(offset,page_type,obj_id,scn,page_no,file_no,col_sum) values(?,?,?,?,?,?,?)",
                                (offset, data2[1], data2[2], 0, sum, 0, 0))
                            if sum % 100000 == 0:
                                conn.commit()  # 每扫到800M的页面时commit一次, 频繁提交会影响I/O速度　
            elif (i == loop_1):  # 处理文件尾部不足buffer的块
                for ii in range(0, loop_2_1):
                    offset = i * buff_size + ii * scan_size + start  # ========
                    if offset > f_size - page_size + start:
                        break
                    pos1 = ii * scan_size  #
                    data1 = data[pos1:pos1 + page_size]
                    fmt = fmt_1 + 'II6HI'
                    data2 = s(fmt, data1[0:24])
                    chk2 = ora_check.ora_page_check_2(data1, fmt_1)  # 页头特征值校验
                    if chk2 == 1:
                        chk3 = ora_check.ora_page_check_3(data1, fmt_1)  #
                        if chk3 == 1:
                            sum += 1
                            ii += page_size // scan_size
                            cursor.execute(
                                "insert into ora_page(offset,page_type,obj_id,scn,page_no,file_no,col_sum) values(?,?,?,?,?,?,?)",
                                (offset, data2[1], data2[2], 0, sum, 0, 0))

            if i % (loop_1 // 1000 + 1) == 0 or i == loop_1:  # 输出扫描进度
                progress = ((i + 1) / (loop_1 + 1)) * 100
                now = time.time()
                speed = i * 8 / (now - begin + 1)
                print("Buf:%d/%d,Percent:%4.1f%%, Find:%d=%dM, I/O:%4.1fM/s,Time:%dMin\r" % (
                i, loop_1, progress, sum, sum * page_size / 1024 / 1024, speed,
                (loop_1 + 1 - i) * 8 / ((speed + 0.01) * 60)), end="")
                self.PUP.emit(progress)  # 传递参数
                self.LUP.emit(" Buf:%d/%d,Percent:%4.1f%%, Find:%d=%dM, I/O:%4.1fM/s,Time:%dMin" % (
                i, loop_1, progress, sum, sum * page_size / 1024 / 1024, speed,
                ((loop_1 + 1 - i) * 8 / ((speed + 0.01) * 60) + 1)))  # 传递参数

        conn.commit();
        cursor.close();
        conn.close();
        f.close()
        end = time.time()
        print("\n总页数:%d " % (sum))  # 总页数,空页数,数据页数
        print("Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end)), end='')  # current time
        print("\t Used time: %d:" % ((end - begin) // 3600) + time.strftime('%M:%S', time.localtime(end - begin)))  # 用时
        print("File size:%6.2fG, 平均I/O:%4.1fM/s" % (
        f_size / 1024 / 1024 / 1024, f_size / 1024 / 1024 / (end - begin + 1)))  # I/O
        logging.info("\n总页数:%d" % (sum))
        logging.info("Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end)) + "\t 扫描完成,用时: %d:" % (
                    (end - begin) // 3600) + time.strftime('%M:%S', time.localtime(end - begin)))
        logging.info("File size:%6.2fG, 平均I/O:%4.1fM/s" % (
        f_size / 1024 / 1024 / 1024, f_size / 1024 / 1024 / (end - begin + 1)))

    # 获取数据页中记录的总列数
    def get_col_sum(self, in_data, fmt_1):
        col_sum = 0
        if fmt_1 == '<':
            t_3 = s('B', in_data[36:37])
        else:
            t_3 = s('B', in_data[37:38])
        pos_1 = 45 + (t_3[0]) * 24 + 8
        try:
            sum_1 = s(fmt_1 + 'BH', in_data[pos_1:(pos_1 + 3)])  # 页面中表数量和记录数量
        except struct.error:
            return 0
        tab_sum = sum_1[0]  # 表数量
        rec_sum = sum_1[1]  # 记录数
        if rec_sum == 0:
            return 0
        pos_2 = pos_1 + 13 + tab_sum * 4
        try:
            slot1 = s(fmt_1 + 'H', in_data[pos_2:pos_2 + 2])  # slot_off_list
            off_set = slot1[0] + pos_1 - 1
            header1 = s("3B", in_data[off_set:off_set + 3])  # 记录头
        except struct.error:
            return 0
        header_0 = header1[0]  # 记录类型
        header_2 = header1[2]  # 记录列数
        if header_0 != 0xAC and header_0 != 0x6C and header_0 != 0x3C:  # 普通记录
            col_sum = header_2
        return col_sum

# f_name = "\\\\.\\PhysicalDrive1"   # 打开磁盘1，即第二块硬盘
# f_name = r'C:\Users\zsz\PycharmProjects\oracle\test\AAA.DBF'   # 数据源路径
# db_name = r'F:\rman\WHOLE1.db'      # 输出数据库路径
# ora_scan(f_name,db_name,0,0,8192,512)
