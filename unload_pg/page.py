# -*- coding: utf-8 -*-
# page ,  record
import struct
import table_struct, data_type

s = struct.unpack  # B,H,I


# 初始化页面
def page_init(data):  # 初始化页面信息
    page1 = table_struct.st_page()
    data1 = s("<2I6HI", data[0:24])
    page1.check = data1[2]
    page1.flagBits = data1[3]
    page1.pd_lower = data1[4]
    page1.rec_sum = (page1.pd_lower - 24) // 4
    for i in range(page1.rec_sum):
        slot1 = s("<I", data[24 + i * 4:28 + i * 4])  # slot_off_list
        slot = table_struct.st_slot()
        slot.len = slot1[0] >> 17  # 高15位，len
        slot.flags = (slot1[0] >> 15) % 4
        slot.off = slot1[0] & 0x00007fff
        page1.page_slot.append(slot)  # 一般每个页面里slot数量在 0-226.
    return page1


# 正常记录解析
def record(f, in_data, table):  # 解析 compact 记录  in_data(输入页面数据),table(表结构)
    page1 = page_init(in_data)  # 初始化页面
    print('rec_sum:%s' % (page1.rec_sum))
    for i in range(page1.rec_sum):  # 页中的所有记录
        record1 = table_struct.st_record()
        off_set = page1.page_slot[i].off  # 记录偏移
        re_len = page1.page_slot[i].len
        flag = page1.page_slot[i].flags
        if flag == 0:  # unused recode
            continue
        try:
            header1 = s("<3I5HB", in_data[off_set:off_set + 23])  # 记录头
        except struct.error:
            continue

        page_no = header1[3] * 65536 + header1[4]
        re_no = header1[5]
        record1.col_sum = header1[6] & 0x00ff  # 此记录的总列数
        len_header = header1[8]  # 记录头的总长度

        print('i:%d,off:%d,re_len:%d,flag:%d,col_sum:%d,len_header:%d,page_no:%d,re_no:%d' % (
        i, off_set, re_len, flag, record1.col_sum, len_header, page_no, re_no))
        continue

        # 解析记录的各列数据, 行溢出先略,  ======================================
        col_off = off_set + 4
        var_list = 0
        bit_1 = 0;
        bit_2 = 0;
        data_out = 0
        for ii3 in range(len(table.col_1)):
            len1 = table.col_1[ii3].col_len  # 列数据类型长度
            col_is_null = record1.null_map[ii3]  # 列是否为空
            if col_is_null == 0:  # 如果列 不为空
                if table.col_1[ii3].varlen_is == 0:  # 是定长
                    if table.col_1[ii3].col_x_type in (3, 239):  # nchar, ntext
                        len1 = len1
                    elif table.col_1[ii3].col_x_type == 104:  # bit
                        if bit_1 % 8 == 0:
                            len1 = 1
                            bit_2_1 = s('B', in_data[col_off:col_off + len1])
                            bit_2 = bit_2_1[0]
                        else:
                            len1 = 0
                        a = bit_2 >> bit_1
                        if bit_1 >= 8:
                            a = bit_2 >> (bit_1 - 8)
                        col_is_true = a % 2
                        if col_is_true == 1:
                            data_out = 'true'
                        elif col_is_true == 0:
                            data_out = 'false'
                        bit_1 += 1
                        col_off += len1
                        record1.col_data2.append(data_out)
                        continue

                elif table.col_1[ii3].varlen_is == 1:  # 是变长
                    if var_list == 0:
                        col_off += null_map_len + 4 + record1.var_col_sum * 2
                        try:
                            len1 = off_set + record1.var_col_off[var_list] - col_off
                        except IndexError:
                            print('变长数组溢出，page_no:%s,off_set:%s' % (page1.page_no, off_set))
                            data_out = ''
                            record1.col_data2.append(data_out)
                            var_list = var_list + 1
                            col_off += len1
                            continue
                    elif var_list > 0:
                        try:
                            len1 = record1.var_col_off[var_list] - record1.var_col_off[var_list - 1]  # 常出错地方
                        except IndexError:
                            print('变长数组溢出，page_no:%s,off_set:%s' % (page1.page_no, off_set))
                            data_out = ''
                            record1.col_data2.append(data_out)
                            var_list = var_list + 1
                            col_off += len1
                            continue

                    if record1.var_col_over[var_list] == 1:  # col溢出
                        col_data_1 = in_data[col_off:col_off + len1]
                        print('col溢出: page_no:%d,len1:%d,col_off:%d' % (page1.page_no, len1, col_off))
                        data_out = col_over(f, col_data_1, table.col_1[ii3])
                        record1.col_data2.append(data_out)
                        var_list = var_list + 1
                        col_off += len1
                        continue

                    var_list = var_list + 1
                col_data_1 = in_data[col_off:col_off + len1]
                col_data2 = data_type.data_type(col_data_1, table.col_1[ii3])  # 列数据解析
                col_off += len1
            elif col_is_null == 1:  # 如果列为空
                if table.col_1[ii3].varlen_is == 0:  # 是定长
                    if table.col_1[ii3].col_x_type in (3, 239):
                        len1 = len1
                    elif table.col_1[ii3].col_x_type == 104:  # bit
                        if bit_1 % 8 == 0:
                            len1 = 1
                            bit_2_1 = s('B', in_data[col_off:col_off + len1])
                            bit_2 = bit_2_1[0]
                        else:
                            len1 = 0
                        bit_1 += 1
                elif table.col_1[ii3].varlen_is == 1:  # 是变长
                    var_list = var_list + 1
                    len1 = 0
                col_data2 = ''  # 列数据解析
                col_off += len1
            record1.col_data2.append(col_data2)

        for i1 in range(table.col_sum):  # 调整列顺序
            for i2 in range(table.col_sum):
                if table.col[i1].col_id == table.col_1[i2].col_id:
                    try:
                        record1.col_data1.append(
                            record1.col_data2[i2])  # col_data1 是最终解析好的，可直接输出的，跟表结构相同。col_data2是记录存储中的列顺序。
                    except IndexError:
                        #     print('溢出，page_no:%s,off_set:%s'%(page1.page_no,off_set))
                        continue
                    break
        page1.record.append(record1)  # 把记录放到 页面的记录容器里,会很多
    return page1


# 　列数据溢出
def col_over(f, data, col_1):
    data_out = ''
    if col_1.col_x_type in (34, 99, 35):  # text,ntext,img
        rowid = s("<IHH", data[8:16])
        file_no = rowid[1]
        page_no = rowid[0]
        slot_no = rowid[2]
        #      print(page_no,slot_no)
        f.seek(page_no * 8192)
        in_data = f.read(8192)
        page1, in_data = page_init(in_data)
        #   print(page1.page_no,page1.page_slot)
        if page1.page_type != 3:
            return data_out
        off = page1.page_slot[slot_no]
        rec_over_h = s("<HHQH", in_data[off:off + 14])
        rec_len = rec_over_h[1]
        rec_type = rec_over_h[3]
        if rec_type == 3:  # DATA
            data = in_data[off + 14:off + rec_len]
            try:
                if col_1.col_x_type == 35:
                    data_out = str(data, encoding="gbk").rstrip()
                else:
                    data_out = str(data, encoding="utf-16").rstrip()
            except UnicodeDecodeError:
                data_out = ''
        elif rec_type == 0:  # small
            rec_len_1 = s("<H", in_data[off + 14:off + 16])
            data = in_data[off + 20:off + 20 + rec_len_1[0]]
            try:
                if col_1.col_x_type == 35:
                    data_out = str(data, encoding="gbk").rstrip()
                else:
                    data_out = str(data, encoding="utf-16").rstrip()
            except UnicodeDecodeError:
                data_out = ''
        elif rec_type == 5:  # large
            data_1 = ''
            rec_h_1 = s("<HH", in_data[off + 14:off + 18])
            MaxLinks = rec_h_1[0]  # 最大链接数
            CurLinks = rec_h_1[1]  # 实际链接数
            for i in range(CurLinks):
                rec_h_2 = in_data[off + 20 + i * 12:off + 36 + i * 12]
                data_out_1 = col_over(f, rec_h_2, col_1)
                #    print(data_out_1)
                data_1 = data_1 + data_out_1
            data_out = data_1
    elif col_1.col_x_type in (165, 231, 167):  # varchar,nvarchar,varbinary
        data_1 = ''
        len_1 = len(data) // 12
        for i in range(1, len_1):
            rec_h_2 = data[(i * 12 - 4):(i + 1) * 12]
            rowid = s("<IHH", rec_h_2[8:16])
            id = col_1.col_x_type
            if col_1.col_x_type == 167:
                col_1.col_x_type = 35
            else:
                col_1.col_x_type = 99
            data_out_1 = col_over(f, rec_h_2, col_1)
            col_1.col_x_type = id
            data_1 = data_1 + data_out_1
        #    print(data_1)
        data_out = data_1

        print('varchar 行溢出，')

    return data_out
