# -*- coding: utf-8 -*-
# 有系统表的从系统表中读取表结构信息放到 sqlite中(或不取),没有系统表的从sql文件或ini文件中获取系统表信息存储到sqlite中,
# 再 从sqlite库里读取表结构到结构体中 ,来初始化表结构. sqlite中的 table 和 column表
import sqlite3, os.path, struct
import page, table_struct

s = struct.unpack


# 解析首页
def file_blk_0(data):
    file_info = table_struct.File_Info()
    pd_pagesize_version = s("<H", data[18:20])
    if pd_pagesize_version[0] < 2048:
        file_info.endian = '>'
        pd_pagesize_version = s(">H", data[18:20])
    else:
        file_info.endian = '<'
    file_info.version = pd_pagesize_version[0] % 1024
    file_info.blk_size = pd_pagesize_version[0] - file_info.version
    # 版本号：从8.4 后都为4
    return file_info


# 链式解析page
def page_link(f, pgfirst, table):  # 文件，要解析的页链的起始页面号，表
    page_size = 8192
    f_size = os.path.getsize(f.name)  # 获取文件大小，只能处理文件,不能磁盘
    f_pg_size = f_size // 8192
    next_page = pgfirst
    tab_data = []
    while (next_page != 0 and next_page < f_pg_size):  # 下一页不溢出
        pos1 = (next_page) * page_size
        f.seek(pos1)  # 要判断返回值，是否成功
        data = f.read(page_size)
        data1 = data[0:page_size]
        page1 = page.record(f, data1, table)  # 解析页面记录.  ***************
        tab_data.append(page1)
        next_page = next_page + 1
        if len(tab_data) > 500:
            next_page = 0
    return tab_data


# 初始化数据库信息，包含07，09表结构
db1 = table_struct.st_db()


def init_dict(version):
    # 读取sqlite数据库表，获得配置信息 和 表结构信息
    db = './db_info.db'  # 数据库文件名称
    conn = sqlite3.connect(db)  # 打开数据库
    cursor = conn.cursor()
    cursor.execute("select * from tab_info ")
    values_tab = cursor.fetchall()  # 返回数据为二维数组
    db1.tab_sum = len(values_tab)
    for i in range(db1.tab_sum):  # 初始化表结构
        table1 = table_struct.st_table()
        table1.tab_obj_id = values_tab[i][1]
        table1.auid_obj_id = table1.tab_obj_id
        table1.tab_name = values_tab[i][3]
        table1.col_sum = values_tab[i][4]
        table1.nullable_sum = values_tab[i][5]
        cursor.execute(
            "select c.* from tab_info t,col_info c where c.tab_obj_id = t.tab_obj_id and t.tab_obj_id=%d order by c.col_id" % table1.tab_obj_id)
        values_col = cursor.fetchall()
        if len(values_col) == 0:
            table1.col_sum = 0
        var_len_sum = 0  # 变长列数
        for ii in range(0, table1.col_sum):
            column1 = table_struct.st_column()
            column1.tab_obj_id = values_col[ii][1]
            column1.col_id = values_col[ii][2]
            column1.col_name = values_col[ii][3]
            column1.col_x_type = values_col[ii][4]
            column1.col_u_type = values_col[ii][5]
            column1.col_len = values_col[ii][6]
            column1.varlen_is = values_col[ii][12]
            table1.col.append(column1)
            if column1.varlen_is == 0:
                table1.col_1.append(column1)
            elif column1.varlen_is == 1:
                var_len_sum = var_len_sum + 1
        table1.var_len_sum = var_len_sum
        for ii in range(0, table1.col_sum):
            if table1.col[ii].varlen_is == 1:
                column1 = table1.col[ii]
                table1.col_1.append(column1)
        if version in (2005, 2012) and table1.tab_obj_id == 7:  # sql2005/2012 的 07 表少一列
            del table1.col_1[-1]
            del table1.col[-1]  # 删除最后一个
            table1.col_sum -= 1
        db1.tab.append(table1)
    cursor.close()
    conn.close()


# 初始化07表
def init_07(f, pgfirst, version):
    init_07_29(version)  # 初始化 db1
    tab_data_07 = page_link(f, pgfirst, db1.tab[0])
    return tab_data_07


# 初始化29表
def init_29(f, tab_data_07):
    for i in range(len(tab_data_07)):
        for ii in range(len(tab_data_07[i].record)):
            if tab_data_07[i].record[ii].col_data1[2] == 281474979397632 and tab_data_07[i].record[ii].col_data1[
                1] == 1:  # 29 表
                pgfirstiam = tab_data_07[i].record[ii].col_data2[7]  # 位图式
                tab_data_29 = IAM_info(f, pgfirstiam, db1.tab[1])
    return tab_data_29


# 初始化 22,05 系统表
def init_22_05(f, tab_data_07, tab_data_29):
    tab_data_22 = 0
    tab_data_05 = 0
    for i in range(len(tab_data_07)):
        for ii in range(len(tab_data_07[i].record)):
            if tab_data_07[i].record[ii].col_data1[2] == 281474978938880 and tab_data_07[i].record[ii].col_data1[
                1] == 1:  # 22 表
                pgfirstiam = tab_data_07[i].record[ii].col_data2[7]
                tab_22 = tab_info(tab_data_29, 34, '', 4)
                tab_22.auid_obj_id = 34
                tab_data_22 = IAM_info(f, pgfirstiam, tab_22)
            if tab_data_07[i].record[ii].col_data1[2] == 327680 and tab_data_07[i].record[ii].col_data1[1] == 1:  # 05 表
                pgfirstiam = tab_data_07[i].record[ii].col_data2[7]
                tab_05 = tab_info(tab_data_29, 5, '', 4)
                tab_05.auid_obj_id = 5
                tab_data_05 = IAM_info(f, pgfirstiam, tab_05)
            # if  tab_data_07[i].record[ii].col_data1[2] == 281474979987456 and tab_data_07[i].record[ii].col_data1[1] == 1 : # 32 表
            #     pgfirstiam = tab_data_07[i].record[ii].col_data2[7]
            #     tab_32 = tab_info(tab_data_29,50,'',4)
            #     tab_data_32 = IAM_info(f,pgfirstiam,tab_32)
    return tab_data_22, tab_data_05


# 通过29表和obj_id初始化 普通表的结构
def tab_info(tab_data_29, obj_id, tab_name, user_id):
    table1 = table_struct.st_table()
    table1.tab_obj_id = obj_id
    table1.tab_name = tab_name
    table1.user_id = user_id
    type = {35: 'text', 34: 'image', 175: 'char', 167: 'varchar', 239: 'nchar', 231: 'nvarchar', 99: 'ntext',
            40: 'date', 41: 'time', \
            42: 'datetime2', 43: 'datetimeoffset', 61: 'datetime', 48: 'tinyint', 52: 'smallint', 56: 'int',
            58: 'smalldatetime', \
            59: 'real', 122: 'smallmoney', 127: 'bigint', 62: 'float', 106: 'decimal', 60: 'money', 189: 'timestamp',
            165: 'varbinary', \
            108: 'numeric', 173: 'binary', 36: 'uniqueidentifier', 98: 'sql_variant', 104: 'bit', 241: 'xml',
            256: 'sysname'}  # 临时的处理办法
    for i in range(len(tab_data_29)):
        loop1 = len(tab_data_29[i].record)  # tab_data_29[i].rec_sum
        for ii in range(loop1):
            if tab_data_29[i].record[ii].col_data1[0] == obj_id:  #
                column1 = table_struct.st_column()
                column1.tab_obj_id = tab_data_29[i].record[ii].col_data1[0]
                column1.col_id = tab_data_29[i].record[ii].col_data1[2]  # 列号，从1编号
                column1.col_name = tab_data_29[i].record[ii].col_data1[3]
                column1.col_x_type = tab_data_29[i].record[ii].col_data1[4]
                column1.col_u_type = tab_data_29[i].record[ii].col_data1[5]
                column1.col_type = type[column1.col_x_type]
                column1.col_len = tab_data_29[i].record[ii].col_data1[6]
                column1.prec = tab_data_29[i].record[ii].col_data1[7]
                column1.scale = tab_data_29[i].record[ii].col_data1[8]
                null = {0: 'null', 1: 'not null'}  # 是否可为空
                column1.nullable_is = null[tab_data_29[i].record[ii].col_data1[10] % 2]
                if column1.col_x_type in (165, 167, 173, 175, 231, 239):
                    column1.col_type = type[column1.col_x_type] + '(%s)' % column1.col_len
                    if column1.col_len == -1:
                        column1.col_type = type[column1.col_x_type] + '(max)'
                    if column1.col_x_type in (231, 239):
                        if column1.col_len > 4000 and column1.col_len <= 8000:
                            column1.col_type = type[column1.col_x_type] + '(%s)' % (int(column1.col_len / 2))
                if column1.col_x_type in (106, 108):
                    column1.col_type = type[column1.col_x_type] + '(%s,%s)' % (column1.prec, column1.scale)
                column1.varlen_is = 0
                table1.col.append(column1)
                if column1.col_x_type not in (34, 35, 98, 99, 129, 130, 165, 167, 231, 241):  # 定长
                    table1.col_1.append(column1)
    table1.col_sum = len(table1.col)
    table1.col.sort(key=lambda x: (x.col_id))  # 排序 。支持多关键字排序
    table1.col_1.sort(key=lambda x: (x.col_id))  # 排序 。支持多关键字排序
    var_len_sum = 0  # 变长列数
    ss = 'create table ' + table1.tab_name + '('
    for ii in range(0, table1.col_sum):
        if table1.col[ii].col_x_type in (34, 35, 98, 99, 129, 130, 165, 167, 231, 241):  # 处理变长列
            column1 = table1.col[ii]
            column1.varlen_is = 1
            table1.col_1.append(column1)
            var_len_sum = var_len_sum + 1

        if ii == table1.col_sum - 1:  # 建表语句
            ss += "[%s] %s);" % (table1.col[ii].col_name, table1.col[ii].col_type)
        else:
            ss += '[%s] %s,' % (table1.col[ii].col_name, table1.col[ii].col_type)
    table1.sql = ss
    table1.var_len_sum = var_len_sum

    return table1


# 初始化所有表
def init_all(f, tab_data_22, tab_data_29, tab_data_05, tab_data_07):
    tab_all = [];
    view_all = [];
    program_all = [];
    trigger_all = [];
    default_all = []
    for i in range(0, len(tab_data_22)):
        for i1 in range(0, tab_data_22[i].rec_sum):
            obj_id = tab_data_22[i].record[i1].col_data1[0]  # object_id，内部id
            obj_name = tab_data_22[i].record[i1].col_data1[1]
            user_id = tab_data_22[i].record[i1].col_data1[2]  # nsid 用户ID
            obj_type = tab_data_22[i].record[i1].col_data1[5]
            if obj_type in ('U', 'S'):  # 获取所有表的表结构   60：sysobjvalues
                tab = tab_info(tab_data_29, obj_id, obj_name, user_id)
                tab.pgfirst, tab.pgfirstiam, tab.auid_obj_id, tab.auid_ind_id = init_all_1(obj_id, tab_data_05,
                                                                                           tab_data_07)
                print('tab_name:%s, object_id:%d, auid_obj_id:%d,auid_ind_id:%d\n' % (
                obj_name, obj_id, tab.auid_obj_id, tab.auid_ind_id))
                tab_all.append(tab)
            elif obj_type == 'V':  # 视图
                view = table_struct.st_view()
                view.view_name = obj_name
                view.view_id = obj_id
                view.user_id = user_id
                view_all.append(view)
            elif obj_type in ('P', 'FN', 'IF', 'TF', 'X'):  # 存储过程
                program = table_struct.st_program()
                program.program_name = obj_name
                program.program_id = obj_id
                program.user_id = user_id
                program_all.append(program)
            elif obj_type == 'TR':  # 触发器
                view = table_struct.st_view()
                view.view_name = obj_name
                view.view_id = obj_id
                view.user_id = user_id
                trigger_all.append(view)
            elif obj_type == 'D':  # 默认值
                view = table_struct.st_view()
                view.view_name = obj_name
                view.view_id = obj_id
                view.user_id = user_id
                default_all.append(view)

    tab_all.sort(key=lambda x: (x.tab_name))  # 按表名 排序 。支持多关键字排序
    view_all.sort(key=lambda x: (x.view_name))  # 按 名 排序 。支持多关键字排序
    program_all.sort(key=lambda x: (x.program_name))  # 按 名 排序 。支持多关键字排序
    trigger_all.sort(key=lambda x: (x.view_name))  # 按 名 排序 。支持多关键字排序
    default_all.sort(key=lambda x: (x.view_name))  # 按 名 排序 。支持多关键字排序
    return tab_all, view_all, program_all, trigger_all, default_all


def init_all_1(obj_id, tab_data_05, tab_data_07):  # 通过05和07表定位
    rowsetid = 0;
    pgfirst = 0;
    pgfirstiam = 0;
    pgroot = 0;
    auid = 0;
    pcdata = 0;
    pcused = 0;
    rcrows = 0;
    auid_obj_id = 0;
    auid_ind_id = 0
    for i in range(len(tab_data_05)):
        for i1 in range(0, tab_data_05[i].rec_sum):
            if obj_id == tab_data_05[i].record[i1].col_data1[2] and tab_data_05[i].record[i1].col_data1[3] in (
            0, 1):  # idminor取0,1
                rowsetid = tab_data_05[i].record[i1].col_data1[0]
                rcrows = tab_data_05[i].record[i1].col_data1[7]
                break
    for i in range(len(tab_data_07)):
        for i1 in range(len(tab_data_07[i].record)):
            if rowsetid == tab_data_07[i].record[i1].col_data1[2] and tab_data_07[i].record[i1].col_data1[
                1] == 1:  # 行内数据
                pgfirst = tab_data_07[i].record[i1].col_data1[5]
                pgroot = tab_data_07[i].record[i1].col_data1[6]
                pgfirstiam = tab_data_07[i].record[i1].col_data1[7]
                pcused = tab_data_07[i].record[i1].col_data1[8]
                pcdata = tab_data_07[i].record[i1].col_data1[9]
                auid = tab_data_07[i].record[i1].col_data1[0]
                auid_obj_id = (auid >> 16) & 0xFFFFFFFF  # 从 AUID 中 解出来的 obj_id
                auid_ind_id = (auid >> 48)  # 从 AUID 中 解出来的 ind_id

    print('05_07_info: rowsetid:%s/%d,auid:%s/%d,pgfirst:%d,pgroot:%d,pgfirstiam:%d,pcused:%d,pcdata:%d,rcrows:%d' % (
    hex(rowsetid), rowsetid, hex(auid), auid, pgfirst, pgroot, pgfirstiam, pcused, pcdata, rcrows))  # 输出 空间管理信息 （05,07）
    return pgfirst, pgfirstiam, auid_obj_id, auid_ind_id


def unload_tab(f, tab):  # 解析指定表
    pgfirstiam = tab.pgfirstiam
    tab_data = IAM_info(f, pgfirstiam, tab)  # 以IAM 位图 式 来解析

    # 解析 删除记录的表(delete)
    # if tab.tab_name in ('kaoshi','dd'):       # 指定要解析的表
    #     tab_data = IAM_info_2(f,pgfirstiam,tab)         # 以IAM 位图 式 来解析 删除记录的表数据
    #     tab_data = test1.page_scan(f,tab)               # 以全文件扫描页面 来解析 删除记录的表数据
    return tab_data
