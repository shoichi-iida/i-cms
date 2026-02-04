# -*- coding: utf-8 -*-

import traceback
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from functions.page.base_page import BasePage
from tornado.options import options
from functions.define.menu_define import MenuDefine

class Page(BasePage):
	def __init__(self):
		super().__init__()
		self.page_handler.extend([
			"operation_list",
		])
		# MENUID, TITLE, ACTION, DISABLE, ADMIN, AUTH, ICON, ORDER, T_COLOR, B_COLOR
		self.portal_menu = MenuDefine("MA002", "操作履歴閲覧", "operation_list", False, False, "ref_operation", "history", 2, "black-text", "orange lighten-3")
		# ログイン状態である必要があるか
		self.need_login = True
		# need_login=Trueでログインしていなかった場合に表示されるページ
		self.back_page = "index"
		# templateファイルの格納ディレクトリ
		self.page_dir = "operation"

	def post_view(self, handler):
		page = handler.prm_req.get("page", "operation_list")
		if page == "operation_list":
			self.view_list(handler)
		super().view(handler)

	def view_list(self, handler):
		op_date_start = handler.prm_req.get("op_date", dt.now().strftime('%Y-%m'))
		op_date_end = (dt.strptime(op_date_start, '%Y-%m') + relativedelta(months=1)).strftime('%Y-%m')
		ac_date_start = handler.prm_req.get("ac_date", dt.now().strftime('%Y-%m'))
		ac_date_end = (dt.strptime(ac_date_start, '%Y-%m') + relativedelta(months=1)).strftime('%Y-%m')
		lst_op_date = []
		lst_ac_date = []
		for record in handler.ctrl_db["db_control"].exec_sql("SELECT DISTINCT DATE_FORMAT(operation_date, '%Y-%m') as op_date FROM tbl_operation_hist;"):
			lst_op_date.append(record["op_date"])
		for record in handler.ctrl_db["db_control"].exec_sql("SELECT DISTINCT DATE_FORMAT(access_date, '%Y-%m') as op_date FROM tbl_access_hist;"):
			lst_ac_date.append(record["op_date"])
		lst_operation = handler.ctrl_db["db_control"].select("tbl_operation_hist", {}, [], [
			f"operation_date >= '{op_date_start}-01'",
			f"operation_date < '{op_date_end}-01'"
		])
		lst_access = handler.ctrl_db["db_control"].select("tbl_access_hist", {}, [], [
			f"access_date >= '{ac_date_start}-01'",
			f"access_date < '{ac_date_end}-01'"
		])
		handler.prm_cmn["lst_op_date"] = sorted(lst_op_date, key=lambda x: x, reverse=True)
		handler.prm_cmn["lst_ac_date"] = sorted(lst_ac_date, key=lambda x: x, reverse=True)
		handler.prm_cmn["lst_operation"] = sorted(lst_operation, key=lambda x: x["operation_date"], reverse=True)
		handler.prm_cmn["lst_access"] = sorted(lst_access, key=lambda x: x["access_date"], reverse=True)
