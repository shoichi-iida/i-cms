# -*- coding: utf-8 -*-

import re
import traceback
from functions.page.base_page import BasePage
from tornado.options import options
from functions.define.menu_define import MenuDefine

class Page(BasePage):
	def __init__(self):
		super().__init__()
		self.page_handler.extend([
			"personal_setting", 
			"personal_setting_edit", 
			"personal_setting_edit_commit"
		])
		# MENUID, TITLE, ACTION, DISABLE, ADMIN, AUTH, ICON, ORDER, T_COLOR, B_COLOR
		self.portal_menu = MenuDefine("MP001", "個人設定", "personal_setting", False, False, None, "settings", 9001, "black-text", "red lighten-4")
		# ログイン状態である必要があるか
		self.need_login = True
		# need_login=Trueでログインしていなかった場合に表示されるページ
		self.back_page = "index"
		# templateファイルの格納ディレクトリ
		self.page_dir = "personal_setting"

		self.input_check = re.compile(r'^[a-zA-Z0-9\-_]+$')

	def post_view(self, handler):
		handler.prm_cmn["define_auth"] = handler.ctrl_define["auth"]["def"]
		page = handler.prm_req.get("page", "user_setting")
		if page == "personal_setting":
			self.view_personal(handler)
		elif page == "personal_setting_edit":
			self.edit_view(handler)
		elif page == "personal_setting_edit_commit":
			if self.edit_commit(handler):
				handler.prm_req["page"] = "personal_setting"
				self.view_personal(handler)
			else:
				handler.prm_req["page"] = "personal_setting_edit"
				self.edit_view(handler)
		super().view(handler)

	def view_personal(self, handler):
		prm_cmn = handler.prm_cmn
		prm_cmn["account_name"] = prm_cmn.get("account_data", {}).get("name", "")
		prm_cmn["account_type"] = "一般ユーザ"
		if prm_cmn.get("account_data", {}).get("admin", False):
			prm_cmn["account_type"] = "管理者"
		lst_affiliation = []
		for r in handler.ctrl_db["db_control"].select("tbl_group_affiliation", dict_select={
			"account_id": prm_cmn["account_id"]
		}):
			lst_affiliation.append(r["group_name"])
		prm_cmn["group_affiliation"] = "\n".join(lst_affiliation)

	def edit_view(self, handler):
		handler.prm_req["account_password"] = handler.prm_req.get("account_password", handler.prm_cmn["account_password"])
		handler.prm_req["confirm_password"] = handler.prm_req.get("confirm_password", handler.prm_cmn["account_password"])

	def edit_commit(self, handler):
		account_password = handler.prm_req.get("account_password", "")
		confirm_password = handler.prm_req.get("confirm_password", "")
		
		# 入力チェック
		flg = False
		if account_password == "":
			handler.alert_message("CE-0011", ["パスワード"])
			flg = True
		if confirm_password == "":
			handler.alert_message("CE-0011", ["パスワード(確認用)"])
			flg = True
		if account_password != confirm_password:
			handler.alert_message("CE-0023", ["パスワード", "パスワード(確認用)"])
			flg = True
		if flg:
			return False

		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].update(
				"tbl_account", 
				{ "password": account_password },
				{ "id" : handler.prm_cmn["account_id"] }
			)
			# コミット
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("AC-0002")
			handler.set_cookie_value("account_password", account_password)
			return True
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False
