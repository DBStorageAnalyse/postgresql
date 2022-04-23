# -*- coding: utf-8 -*-
# 读取数据库文件进行解析,, 主函数 模块

import struct, time, os.path
import init, page

s = struct.unpack


class Unload_DB():
    def __init__(self):  # 解析函数
        super(Unload_DB, self).__init__()
        self.files = []

    # 文件信息
    def file_init(self, fn):
        self.files = []
        for f_name in fn:  # 每个文件头信息
            f = open(f_name, 'rb')
            data = f.read(20)
            file_info = init.file_blk_0(data)
            file_info.f_name = f_name
            file_info.file = f
            file_info.f_size = os.path.getsize(f_name)
            file_info.blk_sum = file_info.f_size // file_info.blk_size
            self.files.append(file_info)
        return self.files

    def unload_db(self, file_infos, db):
        print("Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))  # current time
        f = file_infos[0].file
        f.seek(0)
        in_data = f.read(8192)
        page1 = page.record(f, in_data, '')
        print("over.... ")

    def unload_db1(self, file_infos, db):
        print("Datetime: " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))  # current time
        first_07 = file_infos[0].boot
        f = file_infos[0].file
        tab_data_07 = init.init_07(f, first_07, 2008)
        print("tab_data_07 over ....")
        tab_data_29 = init.init_29(f, tab_data_07)
        print("tab_data_29 over ....")
        tab_data_22, tab_data_05 = init.init_22_05(f, tab_data_07, tab_data_29)
        print("tab_data_22,tab_data_05 over ....")
        tab_all, view_all, program_all, trigger_all, default_all = init.init_all(f, tab_data_22, tab_data_29,
                                                                                 tab_data_05,
                                                                                 tab_data_07)  # 获得所有表的结构（和数据）
        print("unload all over")

        return tab_all, view_all, program_all, trigger_all, default_all

    def unload_tab(self, file_infos, tab, db):
        f = file_infos[0].file
        tab_data = init.unload_tab(f, tab)
        return tab_data

    # save table
    def save_tab(self, tab_data, tab, db):
        if tab.tab_name != 'sysowners':
            init.save_tab(tab_data, tab, db)
