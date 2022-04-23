# -*- coding: utf-8 -*-
from PyQt5 import QtCore, QtGui, QtWidgets, Qt
import sys
import unload_pg


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        self.init()
        MainWindow.resize(860, 560)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap("./icons/db.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        MainWindow.setWindowIcon(icon)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.gridLayout = QtWidgets.QGridLayout(self.centralwidget)
        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tab = QtWidgets.QWidget()
        self.tabWidget.setMovable(1)
        self.gridLayout_2 = QtWidgets.QGridLayout(self.tab)
        self.groupBox = QtWidgets.QGroupBox(self.tab)
        self.groupBox.setMinimumSize(QtCore.QSize(0, 80))
        self.groupBox.setMaximumSize(QtCore.QSize(16777215, 80))
        self.groupBox.setTitle("")
        self.color = QtGui.QColor(150, 150, 150)  # 颜色(190, 180, 150) (140, 140, 140)　(190, 200, 200)
        self.groupBox.setStyleSheet('QGroupBox{border: 2px groove grey; border-radius:5px;\
        border-style: outset;background-color:%s}' % self.color.name())  # 利用样式
        self.pushButton = QtWidgets.QPushButton(self.groupBox)  # 添加文件
        self.pushButton.setGeometry(QtCore.QRect(20, 30, 75, 23))
        self.pushButton_2 = QtWidgets.QPushButton(self.groupBox)  # 移除文件
        self.pushButton_2.setGeometry(QtCore.QRect(120, 30, 75, 23))
        self.pushButton_3 = QtWidgets.QPushButton(self.groupBox)  # 清空列表
        self.pushButton_3.setGeometry(QtCore.QRect(220, 30, 75, 23))
        self.pushButton_4 = QtWidgets.QPushButton(self.groupBox)  # 开始解析
        self.pushButton_4.setGeometry(QtCore.QRect(320, 30, 75, 23))
        self.pushButton_4.setDisabled(1)
        self.pushButton_5 = QtWidgets.QPushButton(self.groupBox)  # 停止解析
        self.pushButton_4.setDisabled(1)
        self.pushButton_5.setGeometry(QtCore.QRect(420, 30, 75, 23))
        self.gridLayout_2.addWidget(self.groupBox, 1, 0, 1, 1)
        self.tableWidget = QtWidgets.QTableWidget(self.tab)  # file table
        self.gridLayout_2.addWidget(self.tableWidget, 2, 0, 1, 1)
        self.tabWidget.addTab(self.tab, "")
        self.tab_2 = QtWidgets.QWidget()
        self.gridLayout_3 = QtWidgets.QGridLayout(self.tab_2)
        self.progressBar = QtWidgets.QProgressBar(self.tab_2)
        self.progressBar.setMaximumSize(QtCore.QSize(16777215, 4))  # 进度条宽度
        self.progressBar.setProperty("value", 0)
        self.progressBar.setTextVisible(False)
        self.gridLayout_3.addWidget(self.progressBar, 2, 0, 1, 1)
        self.splitter = QtWidgets.QSplitter(self.tab_2)
        self.splitter.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.splitter.setLineWidth(0)
        self.splitter.setOrientation(QtCore.Qt.Horizontal)
        self.splitter.setOpaqueResize(True)
        self.splitter.setHandleWidth(3)  # 分隔条的 宽度
        self.treeWidget1 = QtWidgets.QTreeWidget(self.splitter)  # 标签2的 tree
        self.treeWidget1.setHeaderHidden(1)
        self.tableWidget2 = QtWidgets.QTableWidget(self.splitter)  # 标签2的 表2
        self.tableWidget2.setColumnCount(0)
        self.tableWidget2.setRowCount(0)
        self.gridLayout_3.addWidget(self.splitter, 1, 0, 1, 1)
        self.tabWidget.addTab(self.tab_2, "")
        self.splitter.setStretchFactor(1, 2)  # 设置显示比例

        self.tab_3 = QtWidgets.QWidget()  # 标签3,HEX
        # self.tabWidget.addTab(self.tab_3, "")
        self.tab_4 = QtWidgets.QWidget()  # 标签4,高级信息
        self.tabWidget.addTab(self.tab_4, "")
        self.text_1 = QtWidgets.QTextEdit(self.tab_4)
        self.gridLayout_4 = QtWidgets.QGridLayout(self.tab_4)
        self.gridLayout_4.addWidget(self.text_1, 0, 0, 1, 1)
        self.tab_5 = QtWidgets.QWidget()
        self.label = QtWidgets.QLabel(self.tab_5)
        self.label.setGeometry(QtCore.QRect(50, 70, 86, 116))
        self.label_2 = QtWidgets.QLabel(self.tab_5)
        self.label_2.setGeometry(QtCore.QRect(200, 160, 286, 131))
        self.color = QtGui.QColor(140, 140, 140)  # 关于的颜色 (190, 180, 150)　(190, 200, 200) (140, 140, 140)
        self.label_2.setStyleSheet('QLabel{border:2px groove grey; border-radius:5px;\
        border-style: outset;background-color:%s}' % self.color.name())  #
        self.tabWidget.addTab(self.tab_5, "")
        self.gridLayout.addWidget(self.tabWidget, 0, 0, 1, 1)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 831, 23))
        MainWindow.setMenuBar(self.menubar)

        self.expdbAction = QtWidgets.QAction(MainWindow)  # expdbAction =================
        self.expdbAction.triggered.connect(MainWindow.helpAbout)
        self.expallAction = QtWidgets.QAction(MainWindow)  # expallAction =================
        self.expallAction.triggered.connect(MainWindow.helpAbout)
        self.expinfoAction = QtWidgets.QAction(MainWindow)  # expinfoAction =================
        self.expinfoAction.triggered.connect(MainWindow.helpAbout)
        self.selectallAction = QtWidgets.QAction(MainWindow)  # selectallAction =================
        self.selectallAction.triggered.connect(MainWindow.selectall)
        self.notselectAction = QtWidgets.QAction(MainWindow)  # notselectAction =================
        self.notselectAction.triggered.connect(MainWindow.notselect)

        self.exptab_2_db_Action = QtWidgets.QAction(MainWindow)  # exptab_2_db_Action =================
        self.exptab_2_db_Action.triggered.connect(MainWindow.exptab_2_db)
        self.exptab_2_sql_Action = QtWidgets.QAction(MainWindow)  # exptab_2_sql_Action =================
        self.exptab_2_sql_Action.triggered.connect(MainWindow.helpAbout)

        self.pushButton.clicked.connect(self.fileOpen)  # 添加文件
        self.pushButton_2.clicked.connect(self.fileDel)  # 移除文件
        self.pushButton_3.clicked.connect(self.allDel)  # 清空列表
        self.pushButton_4.clicked.connect(self.unload)  # 开始解析
        self.pushButton_5.clicked.connect(self.helpAbout)  # 停止解析
        self.treeWidget1.itemClicked.connect(self.showSelected)  # 单击事件
        self.treeWidget1.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)  # tree 的 右键菜单开关
        self.treeWidget1.customContextMenuRequested['QPoint'].connect(self.on_treeWidget1)  # tree 的右键菜单
        #   self.tableWidget2.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)      # table2 右键菜单开关
        #    self.tableWidget2.customContextMenuRequested['QPoint'].connect(self.on_tableWidget2)   # table2 右键菜单

        self.retranslateUi(MainWindow)
        self.tabWidget.setCurrentIndex(0)  # 初始显示的标签索引
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Postgres 解析工具  V%s" % self.version))
        self.pushButton.setText(_translate("MainWindow", "添加文件"))
        self.pushButton_2.setText(_translate("MainWindow", "移出文件"))
        self.pushButton_3.setText(_translate("MainWindow", "清空列表"))
        self.pushButton_4.setText(_translate("MainWindow", "开始解析"))
        self.pushButton_5.setText(_translate("MainWindow", "停止解析"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("MainWindow", "选择文件"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("MainWindow", "解析文件"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_3), _translate("MainWindow", "HEX"))
        self.label.setText(_translate("MainWindow", ""))
        about = 'Postgres Unload V%s\t\n\n\n解析Postgres数据文件\n支持Postgres各版本的数据文件\n支持win,linux各平台' % self.version
        self.label_2.setText(_translate("MainWindow", "%s" % about))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_4), _translate("MainWindow", "高级信息"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_5), _translate("MainWindow", "关于"))

        self.expdbAction.setText(_translate("MainWindow", "expdb"))  # 菜单栏  子菜单
        self.expallAction.setText(_translate("MainWindow", "expall"))  # 菜单栏  子菜单
        self.expinfoAction.setText(_translate("MainWindow", "expinfo"))  # 菜单栏  子菜单
        self.selectallAction.setText(_translate("MainWindow", "selectall"))  # 菜单栏  子菜单
        self.notselectAction.setText(_translate("MainWindow", "notselect"))  # 菜单栏  子菜单
        self.exptab_2_db_Action.setText(_translate("MainWindow", "exptab_2_db"))  # 菜单栏  子菜单
        self.exptab_2_sql_Action.setText(_translate("MainWindow", "exptab_2_sql"))  # 菜单栏  子菜单

    def init(self):
        self.version = '1.0.0'
        self.file_infos = []
        self.mdf = unload_pg.Unload_DB()

    def fileOpen(self):
        fn, _ = QtWidgets.QFileDialog.getOpenFileNames(self, "Open Files...", None, "DB-Files (*);;All Files (*)")
        files = self.mdf.file_init(fn)
        for file in files:
            self.file_infos.append(file)
        if len(self.file_infos) == 0:
            return
        self.file_infos.sort(key=lambda x: (x.f_no))  # 排序 磁盘和磁盘组。多关键字排序
        self.file_tab()  # 显示磁盘信息
        self.pushButton_4.setDisabled(0)

    def fileDel(self):
        aa1 = self.tableWidget.currentRow()
        if len(self.file_infos) == 0:
            return
        del self.file_infos[aa1]
        self.file_tab()

    def allDel(self):
        self.file_infos = []
        self.file_tab()
        self.pushButton_4.setDisabled(1)

    def stop_unload(self):
        if self.out_file != 0 and self.out_file.is_alive() == True:
            QtWidgets.QMessageBox.about(self, "提示", "停止解析 ... \n")

    # 显示文件列表
    def file_tab(self):
        self.tableWidget.setColumnCount(12)
        self.tableWidget.setAlternatingRowColors(True)  # 隔行变色
        self.tableWidget.setRowCount(len(self.file_infos))
        header = ['file_path', 'file#', 'type', 'db_name', 'f_size(M)', 'blk_sum', 'blk_size', 'ts#', 'ts_name',
                  'version', 'endian', 'big_ts']
        self.tableWidget.setHorizontalHeaderLabels(header)
        self.tableWidget.setColumnWidth(0, 160);
        self.tableWidget.setColumnWidth(1, 40);
        self.tableWidget.setColumnWidth(2, 36);
        self.tableWidget.setColumnWidth(3, 70);
        self.tableWidget.setColumnWidth(4, 70);
        self.tableWidget.setColumnWidth(5, 70);
        self.tableWidget.setColumnWidth(6, 60);
        self.tableWidget.setColumnWidth(7, 30);
        self.tableWidget.setColumnWidth(8, 80);
        self.tableWidget.setColumnWidth(9, 70);
        self.tableWidget.setColumnWidth(10, 50);
        self.tableWidget.setColumnWidth(11, 50);
        endian = {'<': 'little', '>': 'big'}
        for i in range(len(self.file_infos)):
            file_info1 = self.file_infos[i]
            self.tableWidget.setRowHeight(i, 20)
            self.tableWidget.setItem(i, 0, Qt.QTableWidgetItem("%s" % (file_info1.f_name)))
            self.tableWidget.setItem(i, 1, Qt.QTableWidgetItem("%s" % (file_info1.f_no)))
            self.tableWidget.setItem(i, 2, Qt.QTableWidgetItem("%s" % (file_info1.f_type)))
            self.tableWidget.setItem(i, 3, Qt.QTableWidgetItem("%s" % (file_info1.db_name)))
            self.tableWidget.setItem(i, 4, Qt.QTableWidgetItem("%s" % (file_info1.f_size / 1024 / 1024)))
            self.tableWidget.setItem(i, 5, Qt.QTableWidgetItem("%s" % (file_info1.blk_sum)))
            self.tableWidget.setItem(i, 6, Qt.QTableWidgetItem("%s" % (file_info1.blk_size)))
            self.tableWidget.setItem(i, 7, Qt.QTableWidgetItem("%s" % (file_info1.ts_id)))
            self.tableWidget.setItem(i, 8, Qt.QTableWidgetItem("%s" % (file_info1.ts_name)))
            self.tableWidget.setItem(i, 9, Qt.QTableWidgetItem("%s" % (file_info1.version)))
            self.tableWidget.setItem(i, 10, Qt.QTableWidgetItem("%s" % (endian[file_info1.endian])))

    def unload(self):
        self.tabWidget.setCurrentIndex(1)  # 切换标签
        #  f =open (r'.\d_2.db','w'); f.close()
        self.tables = self.mdf.unload_db(self.file_infos, '')  # 解析 *******
        self.showtree()

    def showtree(self):
        data_1 = self.tables  # 表结构数据 （list）
        Item_db = QtWidgets.QTreeWidgetItem(self.treeWidget1)  # 根节 库名/用户名
        Item_db.setText(0, '%s' % self.file_infos[0].db_name)
        icon_db = QtGui.QIcon()
        icon_db.addPixmap(QtGui.QPixmap("./icons/db.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        Item_db.setIcon(0, icon_db)  # 设置图标
        Item_db.setExpanded(True)  # db结点展开
        data_2 = [];
        self.data_3 = []
        for i in range(len(data_1)):
            if data_1[i].tab_name == 'sysowners':  # 解sysowners表数据
                data1 = self.mdf.unload_tab(self.file_infos, self.tables[i], '')
                for data1_1 in data1:
                    for record in data1_1.record:
                        data_2.append(record.col_data1)
            elif data_1[i].tab_name == 'sysobjvalues':  # 解sysowners表数据
                data2 = self.mdf.unload_tab(self.file_infos, self.tables[i], '')
                for data2_1 in data2:
                    for record in data2_1.record:
                        self.data_3.append(record.col_data1)

        for i in range(len(data_2)):  # 用户
            if data_2[i][0] == 1 or data_2[i][1] == 'sys':  # data_2[i][2]=='U'
                user_id = data_2[i][0]
                print('user_id:%d,user:%s' % (user_id, data_2[i][1]))
                Item_user = QtWidgets.QTreeWidgetItem(Item_db)  # 父节 用户名
                Item_user.setText(0, '%s' % data_2[i][1])
                icon_user = QtGui.QIcon()
                icon_user.addPixmap(QtGui.QPixmap("./icons/users.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
                Item_user.setIcon(0, icon_user)  # 设置图标
                Item_user.setExpanded(True)  # user结点展开
                Item_tab = QtWidgets.QTreeWidgetItem(Item_user)  # 表
                icon_tab = QtGui.QIcon()
                icon_tab.addPixmap(QtGui.QPixmap("./icons/t1.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
                Item_tab.setIcon(0, icon_tab)  # 设置图标

                tab_sum = 0;
                view_sum = 0;
                program_sum = 0;
                trigger_sum = 0;
                default_sum = 0
                for table in data_1:  # 用户下的表
                    if table.user_id == user_id:
                        Item1 = QtWidgets.QTreeWidgetItem(Item_tab)  # 父节 表名
                        Item1.setIcon(0, icon_tab)  # 设置图标
                        tab_sum += 1;
                        col_sum = 0
                        for col in table.col:  # 表的列
                            Item1_1 = QtWidgets.QTreeWidgetItem(Item1)  # 子节 列
                            try:
                                Item1_1.setText(0, '%s' % (col.col_name + ' (%s,%s)' % (col.col_type, col.nullable_is)))
                            except KeyError:
                                Item1_1.setText(0, '%s' % (col.col_name + ' (unkown,%s)' % (col.nullable_is)))
                            Item1.addChild(Item1_1)  # 添加子节点
                            col_sum += 1
                        Item1.setText(0, '%s' % (table.tab_name))  # 表名
                Item_tab.setText(0, '%s(%d)' % ('tables', tab_sum))

                Item_view = QtWidgets.QTreeWidgetItem(Item_user)  # 视图
                Item_view.setIcon(0, icon_tab)  # 设置图标
                for view in self.view_all:
                    if view.user_id == user_id:
                        Item2 = QtWidgets.QTreeWidgetItem(Item_view)
                        Item2.setIcon(0, icon_tab)  # 设置图标
                        view_sum += 1;
                        Item2.setText(0, '%s' % (view.view_name))
                Item_view.setText(0, '%s(%d)' % ('views', view_sum))

                Item_programmability = QtWidgets.QTreeWidgetItem(Item_user)  # programmability
                Item_programmability.setIcon(0, icon_tab)  # 设置图标
                Item_programmability.setText(0, 'programmability')

                Item_program = QtWidgets.QTreeWidgetItem(Item_programmability)  # 存储过程
                Item_program.setIcon(0, icon_tab)  # 设置图标
                for program in self.program_all:
                    if program.user_id == user_id:
                        Item3 = QtWidgets.QTreeWidgetItem(Item_program)
                        Item3.setIcon(0, icon_tab)  # 设置图标
                        program_sum += 1;
                        Item3.setText(0, '%s' % (program.program_name))
                Item_program.setText(0, '%s(%d)' % ('programs', program_sum))

                Item_trigger = QtWidgets.QTreeWidgetItem(Item_programmability)  # 触发器
                Item_trigger.setIcon(0, icon_tab)  # 设置图标
                for trigger in self.trigger_all:
                    if trigger.user_id == user_id:
                        Item4 = QtWidgets.QTreeWidgetItem(Item_trigger)
                        Item4.setIcon(0, icon_tab)  # 设置图标
                        trigger_sum += 1;
                        Item4.setText(0, '%s' % (trigger.view_name))
                Item_trigger.setText(0, '%s(%d)' % ('triggers', trigger_sum))

                Item_default = QtWidgets.QTreeWidgetItem(Item_programmability)  # 默认值
                Item_default.setIcon(0, icon_tab)  # 设置图标
                for default in self.default_all:
                    if default.user_id == user_id:
                        Item5 = QtWidgets.QTreeWidgetItem(Item_default)
                        Item5.setIcon(0, icon_tab)  # 设置图标
                        default_sum += 1;
                        Item5.setText(0, '%s' % (default.view_name))
                Item_default.setText(0, '%s(%d)' % ('defaults', default_sum))

    # tree 的鼠标左键
    def showSelected(self):
        item = self.treeWidget1.currentItem()
        if item == None:
            a = 0
        elif item.parent() == None:  # db 层 菜单
            a = 0
        elif item.parent().parent() == None:  # user 层  菜单
            a = 0
        elif item.parent().parent().parent() == None:  # user 层  菜单
            a = 0
        elif item.parent().text(0)[0:3] == 'tab':  # table 层菜单
            self.tab_show()
        elif item.parent().parent().text(0)[0:3] == 'pro' or item.parent().text(0)[0:3] == 'vie':
            self.programmability_show()

    # table2 表数据显示
    def tab_show(self):
        self.data = [];
        self.data1 = [];
        self.table = 0  # 表的记录数据
        self.tableWidget2.setAlternatingRowColors(True)  # 隔行变色
        self.tableWidget2.setColumnWidth(0, 80)  # 调整行宽
        item = self.treeWidget1.currentItem()
        tab_text1 = item.text(0)  # 获取当前item的文本
        for table in self.tables:
            if table.tab_name == tab_text1:
                print('table:%s' % (table.tab_name))
                self.table = table
                self.data1 = self.mdf.unload_tab(self.file_infos, table, '')
                for data1_1 in self.data1:
                    for record in data1_1.record:
                        self.data.append(record.col_data1)
                if len(self.data) == 0:
                    col_count = table.col_sum;
                    row_count = 0
                else:
                    col_count = table.col_sum  # (self.data[0]) # 列数
                    row_count = len(self.data)  # 行数
                self.tableWidget2.setColumnCount(col_count)
                self.tableWidget2.setRowCount(row_count)
                for i in range(col_count):  # 表头列，设置表头
                    self.tableWidget2.setHorizontalHeaderItem(i, Qt.QTableWidgetItem("%s" % table.col[i].col_name))
                for i in range(row_count):  # 设置表数据
                    self.tableWidget2.setRowHeight(i, 18)  # 调整行高
                    for ii in range(col_count):  # 列
                        try:
                            self.tableWidget2.setItem(i, ii, Qt.QTableWidgetItem("%s" % self.data[i][ii]))
                        except IndexError:
                            break
                break

    # table2 其他对象数据显示
    def programmability_show(self):
        self.tableWidget2.setColumnCount(1)
        self.tableWidget2.setRowCount(1)
        self.tableWidget2.setColumnWidth(0, 600)  # 调整行宽
        self.tableWidget2.setRowHeight(0, 400)  # 调整行高
        item = self.treeWidget1.currentItem()
        tab_text1 = item.text(0)  # 获取当前item的文本
        tab_text2 = item.parent().text(0)
        tab_text2 = tab_text2[0:3]
        id = 0
        if tab_text2 == 'pro':
            for program in self.program_all:
                if program.program_name == tab_text1:
                    id = program.program_id
        elif tab_text2 == 'tig':
            for trigger in self.trigger_all:
                if trigger.view_name == tab_text1:
                    id = trigger.view_id
        elif tab_text2 == 'def':
            for default in self.default_all:
                if default.view_name == tab_text1:
                    id = default.view_id
        elif tab_text2 == 'vie':
            for view in self.view_all:
                if view.view_name == tab_text1:
                    id = view.view_id

        for i in range(len(self.data_3)):
            if self.data_3[i][1] == id:
                self.tableWidget2.setItem(0, 0, Qt.QTableWidgetItem("%s" % self.data_3[i][5]))

    #  tree的右键菜单
    def on_treeWidget1(self):  # on_treeWidget1_customContextMenuRequested
        # menu_tab0 = QtWidgets.QMenu(self)   #　db 层
        # menu_tab0.addAction(self.expdbAction)
        # menu_tab1 = QtWidgets.QMenu(self)       # 用户层
        # menu_tab1.addAction(self.expall_2_db_Action)
        # menu_tab2 = QtWidgets.QMenu(self)       # tables层
        # menu_tab2.addAction(self.expalltab_2_db_Action)
        # menu_tab2.addAction(self.expalltab_2_sql_Action)
        menu_tab3 = QtWidgets.QMenu(self)  # table层
        menu_tab3.addAction(self.exptab_2_db_Action)
        menu_tab3.addAction(self.exptab_2_sql_Action)
        item = self.treeWidget1.currentItem()

        if item == None:
            a = 0
        elif item.parent() == None:  # db 层 菜单
            a = 0
        elif item.parent().parent() == None:  # user 层  菜单
            a = 0
        elif item.parent().parent().parent() == None:  # user 层  菜单
            a = 0
        else:  # table 层菜单
            menu_tab3.exec_(QtGui.QCursor.pos())

    def exptab_2_db(self):
        item = self.treeWidget1.currentItem()
        tab_text = item.text(0)  # 获取当前item的文本
        #     print('parent:%s,tab_name:%s,%s'%(par,tab_text,self.file_infos[0].db_name))
        if self.table.tab_name != tab_text or len(self.data1) == 0:
            self.tab_show()
        self.mdf.save_tab(self.data1, self.table, '')

    # table2 的右键菜单
    def on_tableWidget2(self, pos):
        menu_1 = QtWidgets.QMenu(self)
        menu_1.addAction(self.expfileAction)
        menu_1.addAction(self.selectallAction)
        menu_1.addAction(self.notselectAction)
        item = self.tableWidget2.currentItem()
        if item != None:
            menu_1.exec_(QtGui.QCursor.pos())

    def selectall(self):
        self.tableWidget2.selectAll()  # 全选

    def notselect(self):
        self.tableWidget2.clearSelection()  # 清除选择

    def progress(self, progress_val):
        self.progressBar.setValue(progress_val)

    def closeEvent(self, e):
        if self.file_infos == []:
            e.accept()
        else:
            ret = QtWidgets.QMessageBox.question(self, "退出", self.tr(" 是否确定要退出程序 ?\t"),
                                                 QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel,
                                                 QtWidgets.QMessageBox.Ok)
            if ret == QtWidgets.QMessageBox.Ok:
                e.accept()
            else:
                e.ignore()

    def helpAbout(self):  # 测试用
        QtWidgets.QMessageBox.about(self, "About", "\MDF unload  \t\n")


class myui(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        super(myui, self).__init__()
        self.setupUi(self)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    ui = myui()
    ui.show()
    sys.exit(app.exec_())
