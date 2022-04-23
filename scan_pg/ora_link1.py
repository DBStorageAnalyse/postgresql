# -*- coding: utf-8 -*-
# sql server 碎片物理拼接 连续性拼接,半页聚合. 需要读源数据和sqlite库.
from PyQt5.QtCore import QThread, pyqtSignal
import sqlite3, struct, time
import ora_check, logging


# logging.basicConfig(level=logging.DEBUG,format='%(message)s',filename='%s_%s.log'%('aaaa',time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))),filemode='w')

# 扫描线程, 需要传参数
class Link_wl(QThread):  # 类Scan_1继承自 QThread
    PUP = pyqtSignal(int)  # 更新进度条的信号
    LUP = pyqtSignal(str)

    def __init__(self, parent=None):  # 解析函数
        super(Link_wl, self).__init__(parent)
        self.f_name = 0
        self.db_name = ''
        self.page_size = 0
        self.endian = 0
        self.logging = 0

    def run(self):
        self.link_wl(self.f_name, self.db_name, self.endian, self.page_size, self.logging)

    def link_1(self, db_name, page_size, logging):
        print("\nbegin link_1 ...  Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S',
                                                               time.localtime(time.time())))  # current time
        logging.info("\nbegin link_1 ...  Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        conn1 = sqlite3.connect(db_name)
        cursor1 = conn1.cursor()
        cursor1.execute("select offset,page_no,file_no,scn from ora_page ")  # 全部查出来
        page_size = page_size
        print("开始 进入内存,稍等 ...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("开始 进入内存,稍等 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        v_1 = cursor1.fetchall()  # 取出数据, 里边的成员不能修改
        v_2 = []
        if len(v_1) == 0:
            cursor1.execute(
                "create table link_2(g1_id,page_sum,offset_1,offset_2,scn_1,scn_2,page_no_1,page_no_2,file_no_1,blk_sum,g2_id,g2in_id)")
            cursor1.execute(
                "create table link_3(g2_id,page_sum,offset_1,offset_2,scn_1,scn_2,page_no_1,page_no_2,file_no_1,blk_sum)")
            print('没有页碎片,link_1完成 ... ')
            logging.info('没有页碎片,link_1完成 ... ')
            return
        v_1.append(v_1[0])
        print("已经进入内存,开始拼接... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("已经进入内存,开始拼接... " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        for i in range(len(v_1) - 1):  # 开始拼接物理碎片。 太多了，注意性能
            if i % 10000 == 0 or i == len(v_1) - 2:
                progress = ((i + 1) / (len(v_1) - 1)) * 100
                print("拼接进度：%d%% \r" % (progress), end="")
                self.PUP.emit(progress)  # 传递参数
                self.LUP.emit("\tlink_1 : %d%% " % (progress))  # 传递参数

            if i == 0:
                aa_2 = ora_check.values2()
                v_2.append(aa_2)

            #   拼碎片
            bool_1 = (v_1[i + 1][0] - v_1[i][0]) / page_size == (v_1[i + 1][1] - v_1[i][1]) and (
                        v_1[i + 1][1] - v_1[i][1]) > 0 and (v_1[i + 1][1] - v_1[i][1]) <= 8
            # v_1[i][2] == v_1[i+1][2] and    文件号相同
            if bool_1 == 1:
                v_2[-1].offset_2 = v_1[i][0]
                v_2[-1].page_no_2 = v_1[i][1]
                v_2[-1].file_no_2 = v_1[i][2]
                v_2[-1].scn_2 = v_1[i][3]
                if v_2[-1].page_sum == 1:
                    v_2[-1].offset_1 = v_1[i][0]
                    v_2[-1].page_no_1 = v_1[i][1]
                    v_2[-1].file_no_1 = v_1[i][2]
                    v_2[-1].scn_1 = v_1[i][3]
                    v_2[-1].page_sum = 2
                if v_2[-1].file_no_1 == 0:
                    v_2[-1].file_no_1 = v_1[i + 1][2]
                if v_2[-1].file_no_2 == 0:
                    v_2[-1].file_no_2 = v_1[i + 1][2]
            if bool_1 == 0:
                v_2[-1].offset_2 = v_1[i][0]
                v_2[-1].page_no_2 = v_1[i][1]
                v_2[-1].file_no_2 = v_1[i][2]
                v_2[-1].scn_2 = v_1[i][3]
                v_2[-1].page_sum = v_2[-1].page_no_2 - v_2[-1].page_no_1 + 1
                aa_2 = ora_check.values2()  # 下一个碎片开始
                aa_2.offset_1 = v_1[i + 1][0]
                aa_2.page_no_1 = v_1[i + 1][1]
                aa_2.file_no_1 = v_1[i + 1][2]
                aa_2.scn_1 = v_1[i + 1][3]
                aa_2.offset_2 = v_1[i + 1][0]
                aa_2.page_no_2 = v_1[i + 1][1]
                aa_2.file_no_2 = v_1[i + 1][2]
                aa_2.scn_2 = v_1[i + 1][3]
                aa_2.page_sum = 1
                if i != len(v_1) - 2:
                    v_2.append(aa_2)
                del aa_2
        del v_1

        # print("\n开始插入 link_1 表...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        logging.info("开始插入 link_1 表...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        for i2 in range(len(v_2)):
            if v_2[i2].file_no_1 == 0:  # 碎片的起始文件号不正常时记结尾文件号
                v_2[i2].file_no_1 == v_2[i2].file_no_2
            cursor1.execute(
                "insert into link_1(offset_1,offset_2,scn_1,scn_2,page_sum,page_no_1,page_no_2,file_no_1) values(?,?,?,?,?,?,?,?)",
                (v_2[i2].offset_1, v_2[i2].offset_2, v_2[i2].scn_1, v_2[i2].scn_2, v_2[i2].page_sum, v_2[i2].page_no_1,
                 v_2[i2].page_no_2, v_2[i2].file_no_1))
            if i2 % 100000 == 0 or i2 == len(v_2) - 1:
                conn1.commit()
        conn1.commit()
        cursor1.execute("select offset,col_sum from ora_page where page_no=1;")  # 全部查出来
        v_3 = cursor1.fetchall()
        cursor1.execute("select offset_1 from link_1 where page_no_1=1;")  # 全部查出来
        v_4 = cursor1.fetchall()
        for i4 in range(len(v_4)):
            for i3 in range(len(v_3)):
                if v_3[i3][0] == v_4[i4][0]:
                    cursor1.execute("update link_1 set blk_sum=%d where offset_1=%d " % (v_3[i3][1], v_4[i4][0]))
                    break
        conn1.commit()
        cursor1.close();
        conn1.close()
        del v_2
        print("Over link_1 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S',
                                                           time.localtime(time.time())))  # current time
        logging.info("Over link_1 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    def link_2(self, f_name, db_name, endian, page_size, logging):  # 源文件名，sqlite数据库名
        f = open(f_name, 'rb')
        print("\nbegin link_2 ...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("\nbegin link_2 ...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        if endian == 1:  # 大端
            fmt = '>I';
            fmt_1 = '>'
        elif endian == 0:  # 小端
            fmt = '<I';
            fmt_1 = '<'
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("create table link_1_1 as select * from link_1 order by page_no_1")
        cursor.execute("DROP TABLE link_1")
        cursor.execute("ALTER TABLE link_1_1 RENAME TO link_1")
        cursor.execute("select offset_1,offset_2,page_no_1,page_no_2,file_no_1,scn_1,scn_2 from link_1")  # 从前向后拼接
        values = cursor.fetchall()  # 取出数据
        if len(values) == 0:
            return
        v_1 = []  # 就是用来修改的
        page_size = page_size
        print("开始 进入内存. Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("开始 进入内存. Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        for i in range(0, len(values)):
            aa_1 = ora_check.values2()
            aa_1.offset_1 = values[i][0]
            aa_1.offset_2 = values[i][1]
            aa_1.page_no_1 = values[i][2]
            aa_1.page_no_2 = values[i][3]
            aa_1.file_no_1 = values[i][4]
            aa_1.scn_1 = values[i][5]
            aa_1.scn_1 = values[i][6]
            v_1.append(aa_1)
            del aa_1
        v_1.append(v_1[0])
        del values
        g_id = 0
        print("已经进入内存,开始合并. Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("已经进入内存,开始合并. Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

        # 开始拼接
        for i in range(0, len(v_1) - 1):  # 外循环， 注意性能，小心内存。 很慢 ++++++++++++++
            if v_1[i].page_sum == 5:  # 标记 update过的
                continue
            else:
                g_id += 1
                gin_id = 1
                cursor.execute("update link_1 set g1_id=%d,g1in_id=%d,s_pos=%d,e_pos=%d where offset_1=%d " % (
                g_id, gin_id, 0, 0, v_1[i].offset_1))
            offset = v_1[i].offset_2 + page_size
            f.seek(offset)
            data1 = f.read(page_size)  # 取出碎片尾的页, 看它是否为一个半页
            if len(data1) == 0:
                if i == 0:
                    self.PUP.emit(100)  # 传递参数
                    self.LUP.emit("\tlink_2 : %d%% " % (100))  # 传递参数
                continue
            chk1 = ora_check.ora_page_check_2(data1, fmt_1)  # 特征值校验
            if chk1 == 0:
                continue
            pagefile = struct.unpack(fmt, data1[4:8])  # page_no,file_no
            file = pagefile[0] >> 22
            page = pagefile[0] - 4194303 * file - file

            bool = chk1 == 1 and page == v_1[i].page_no_2 + 1 and file in (v_1[i].file_no_1, 0)  # 是否是半页
            for ii in range(i + 1, len(v_1) - 1):  # 内循环
                if bool and v_1[ii].page_no_1 == v_1[i].page_no_2 + 2 and (
                        v_1[ii].file_no_1 in (v_1[i].file_no_1, 0)):  # 文件号相同或0
                    pos = 0  # 进行半页面组合
                    f.seek(v_1[ii].offset_1 - page_size)
                    data2 = f.read(page_size)  # 取出碎片前的半页
                    for i1 in range(0, 15):  # 连接半页
                        data = data1[0:512 * (i1 + 1)] + data2[512 * (i1 + 1):512 * (16)]
                        chk = ora_check.ora_page_check_1(data, 0)
                        if chk == 1:  # 通过校验
                            pos = i1 + 1
                            break
                        else:
                            pos = 0
                    if pos != 0:  # 如果找到了连续的碎片
                        gin_id += 1
                        v_1[i].page_no_2 = v_1[ii].page_no_2
                        cursor.execute("update link_1 set g1_id=%d,g1in_id=%d,s_pos=%d,e_pos=%d where offset_1=%d " % (
                        g_id, gin_id, (16 - pos), 0, v_1[ii].offset_1))
                        cursor.execute("update link_1 set e_pos=%d where offset_1=%d " % (pos, v_1[i].offset_1))
                        v_1[ii].page_sum = 5
                elif v_1[ii].page_no_1 > v_1[i].page_no_2 + 2:
                    break
                elif v_1[ii].page_sum == 5:
                    continue
            if i % 10000 == 0 or i == len(v_1) - 2:  # 每10000条commit一次, 频繁提交会影响I/O速度
                progress = ((i + 1) / (len(v_1) - 1)) * 100
                self.PUP.emit(progress)  # 传递参数
                self.LUP.emit("\tlink_2 : %d%% " % (progress))  # 传递参数
                conn.commit()
        del v_1
        conn.commit();
        cursor.close();
        conn.close();
        f.close()
        self.link_2_1(db_name)  # 生成 link_2 表
        print("Over link_2 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("Over link_2 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    def link_2_1(self, db_name):  # 生成 link_2表
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        try:
            cursor.execute("drop table link_2;")
        except sqlite3.OperationalError:
            pass
        cursor.execute(
            "create table aa1 as select g1_id,file_no_1,page_no_2,offset_2,scn_2 from link_1 group by g1_id;")
        cursor.execute("create table aa2 as select g1_id,offset_1,page_no_1,blk_sum,scn_1 from link_1 where g1in_id=1;")
        cursor.execute(
            "create table link_2 as select b.g1_id,(a.page_no_2-b.page_no_1+1) page_sum,b.offset_1,a.offset_2,b.page_no_1,a.page_no_2,a.file_no_1,b.blk_sum,b.scn_1,a.scn_2 from aa1 a,aa2 b where a.g1_id=b.g1_id;")
        cursor.execute("alter table link_2 add g2_id int;")
        cursor.execute("alter table link_2 add g2in_id int;")
        cursor.execute("drop table aa1;")
        cursor.execute("drop table aa2;")
        conn.commit();
        cursor.close();
        conn.close()

    def link_3(self, db_name, logging):
        print("\nbegin link_3 ...  Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S',
                                                               time.localtime(time.time())))  # current time
        logging.info("\nbegin link_3 ...  Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        conn1 = sqlite3.connect(db_name)
        cursor1 = conn1.cursor()
        cursor1.execute(
            "select offset_1,offset_2,page_no_1,page_no_2,file_no_1,scn_1,scn_2 from link_2 order by page_no_1")  # 全部查出来
        v_1 = [];
        v_2 = []
        print("开始 进入内存,稍等 ...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("开始 进入内存, ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        values = cursor1.fetchall()  # 取出数据, 里边的成员不能修改
        for i in range(0, len(values)):
            aa_1 = ora_check.values2()
            aa_1.offset_1 = values[i][0]
            aa_1.offset_2 = values[i][1]
            aa_1.page_no_1 = values[i][2]
            aa_1.page_no_2 = values[i][3]
            aa_1.file_no_1 = values[i][4]
            aa_1.scn_1 = values[i][5]
            aa_1.scn_2 = values[i][6]
            v_1.append(aa_1)
            del aa_1
        del values
        print("已经进入内存,开始合并 ...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("已经进入内存,开始合并 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        g_id = 0
        for i in range(0, len(v_1)):  # 外循环， 注意性能，小心内存。 很慢 ++++++++++++++
            if v_1[i].page_sum == 5:  # 标记 update过的
                continue
            else:
                g_id += 1
                gin_id = 1
                cursor1.execute(
                    "update link_2 set g2_id=%d,g2in_id=%d where offset_1=%d " % (g_id, gin_id, v_1[i].offset_1))

            for ii in range(i + 1, len(v_1)):  # 内循环
                for ii2 in range(i + 1, len(v_1)):  # 内循环,  找到所有可匹配的
                    if v_1[ii2].page_no_1 == v_1[i].page_no_2 + 1 and v_1[i].file_no_1 == v_1[ii2].file_no_1 and v_1[
                        ii2].page_sum != 5:
                        v_1[ii2].scn_abs = abs(v_1[i].scn_2 - v_1[ii2].scn_1)  # 借用scn_1变量
                        v_1[ii2].no = ii2  # 借用page_no_1变量
                        v_2.append(v_1[ii2])
                    elif v_1[ii2].page_no_1 > v_1[i].page_no_2 + 1:
                        break
                if len(v_2) != 0:
                    v_2.sort(key=lambda x: (x.scn_abs))  # 排序.支持多关键字排序
                    gin_id += 1
                    v_1[i].page_no_2 = v_2[0].page_no_2
                    v_1[i].scn_2 = v_2[0].scn_2
                    v_1[v_2[0].no].page_sum = 5
                    cursor1.execute("update link_2 set g1_id=%d,g2_id=%d,g2in_id=%d where offset_1=%d " % (
                    5, g_id, gin_id, v_2[0].offset_1))
                    #   print('****%d， %d, %d'%(v_1[i].page_no_2,v_2[0].offset_1,v_2[0].page_no_1))
                    v_2 = []
                elif len(v_2) == 0:
                    break

            if i % 10000 == 0 or i == len(v_1) - 1:  # 每10000条commit一次, 频繁提交会影响I/O速度
                progress = ((i + 1) / (len(v_1))) * 100
                self.PUP.emit(progress)  # 传递参数
                self.LUP.emit("\tlink_3 : %d%% " % (progress))  # 传递参数
                conn1.commit()
        conn1.commit();
        cursor1.close();
        conn1.close()
        self.link_3_1(db_name)
        print("Over link_3 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        logging.info("Over link_3 ... Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    def link_3_1(self, db_name):  # 生成 link_3表
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        try:
            cursor.execute("drop table link_3;")
        except sqlite3.OperationalError:
            pass
        cursor.execute("create table aa1 as select g2_id,file_no_1,page_no_2,offset_2 from link_2 group by g2_id;")
        cursor.execute("create table aa2 as select g2_id,offset_1,page_no_1,blk_sum from link_2 where g2in_id=1;")
        cursor.execute(
            "create table link_3 as select b.g2_id,(a.page_no_2-b.page_no_1+1) page_sum,b.offset_1,a.offset_2,b.page_no_1,a.page_no_2,a.file_no_1,b.blk_sum from aa1 a,aa2 b where a.g2_id=b.g2_id;")
        cursor.execute("drop table aa1;")
        cursor.execute("drop table aa2;")
        conn.commit();
        cursor.close();
        conn.close()

    def link_wl(self, f_name, db_name, endian, page_size, logging):  # 物理拼接汇总
        self.link_1(db_name, page_size, logging)  # 物理连续拼接
        self.link_2(f_name, db_name, endian, page_size, logging)  # 半页拼接，需要读数据源
        self.link_3(db_name, logging)  # 页号连续文件号相同 拼接
        print("\n物理拼接(link_1,link_2,link_3)完成...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S',
                                                                            time.localtime(time.time())))
        logging.info("\n物理拼接(link_1,link_2,link_3)完成...Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                   time.localtime(time.time())))
        self.PUP.emit(-2)  # 传递参数
        self.LUP.emit("\tlink_wl over")  # 传递参数

# # # f_name = r'F:\ruyang.img'
# #db_name = r'C:\Users\zsz\PycharmProjects\oracle\scan_dbf\test7\ora_scan.db'
# db_name = r'C:\Users\zsz\Desktop\841\171 suipian_xin\suipian_3\ora_scan.db'
# link_3(db_name,logging)
