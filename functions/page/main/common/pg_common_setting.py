# -*- coding: utf-8 -*-

import re
import uuid
import traceback
from datetime import datetime as dt
from functions.page.base_page import BasePage
from tornado.options import options
from functions.define.menu_define import MenuDefine
from functions.common.util_check import UtilCheck

class Page(BasePage):
	def __init__(self):
		super().__init__()
		self.page_handler.extend([
			"common_setting", 
			"common_setting_edit", 
			"notice_list", 
			"notice_add", 
			"notice_add_commit", 
			"notice_edit", 
			"notice_edit_commit", 
			"notice_delete", 
			"update_list", 
			"update_add", 
			"update_add_commit",
			"update_edit", 
			"update_edit_commit",
			"update_delete"
		])
		# MENUID, TITLE, ACTION, DISABLE, ADMIN, AUTH, ICON, ORDER, T_COLOR, B_COLOR
		self.portal_menu = MenuDefine("MA001", "基本設定", "common_setting", False, True, None, "settings", 1, "black-text", "orange lighten-3")
		# メンテナンス中に使用可能か否か
		self.maintenance_use = True
		# ログイン状態である必要があるか
		self.need_login = True
		# need_login=Trueでログインしていなかった場合に表示されるページ
		self.back_page = "index"
		# templateファイルの格納ディレクトリ
		self.page_dir = "common_setting"

		self.setting_keys = [
			{
				"key" : "maintenance",
				"default" : "0"
			},
			{
				"key" : "maintenance_enddate",
				"default" : ""
			},
			{
				"key" : "maintenance_notice",
				"default" : ""
			}
		]
		self.input_check = re.compile(r'^[a-zA-Z0-9\-_]+$')

	def post_view(self, handler):
		handler.prm_cmn["define_auth"] = handler.ctrl_define["auth"]["def"]
		page = handler.prm_req.get("page", "common_setting")
		if page == "common_setting":
			self.edit_view(handler)
		elif page == "common_setting_edit":
			if self.edit_commit(handler):
				self.edit_view(handler)
			handler.prm_req["page"] = "common_setting"
		elif page == "notice_list":
			handler.prm_cmn["lst_notice"] = handler.ctrl_db["db_control"].select("tbl_notice")
		elif page == "notice_add":
			handler.prm_req["flg_add"] = True
			handler.prm_req["page"] = "notice_edit"
		elif page == "notice_add_commit":
			if self.check_notice_edit(handler) and self.add_notice(handler):
				self.view_notice_edit(handler)
			else:
				handler.prm_req["flg_add"] = True
			handler.prm_req["page"] = "notice_edit"
		elif page == "notice_edit":
			self.view_notice_edit(handler)
		elif page == "notice_edit_commit":
			if self.check_notice_edit(handler) and self.edit_notice(handler):
				self.view_notice_edit(handler)
			handler.prm_req["page"] = "notice_edit"
		elif page == "notice_delete":
			self.delete_notice(handler)
			handler.prm_req["page"] = "notice_list"
			handler.prm_cmn["lst_notice"] = handler.ctrl_db["db_control"].select("tbl_notice")
		elif page == "update_list":
			handler.prm_cmn["lst_update"] = handler.ctrl_db["db_control"].select("tbl_update")
		elif page == "update_add":
			handler.prm_req["flg_add"] = True
			handler.prm_req["page"] = "update_edit"
		elif page == "update_add_commit":
			if self.check_update_edit(handler) and self.add_update(handler):
				self.view_update_edit(handler)
			else:
				handler.prm_req["flg_add"] = True
			handler.prm_req["page"] = "update_edit"
		elif page == "update_edit":
			self.view_update_edit(handler)
		elif page == "update_edit_commit":
			if self.check_update_edit(handler) and self.edit_update(handler):
				self.view_update_edit(handler)
			handler.prm_req["page"] = "update_edit"
		elif page == "update_delete":
			self.delete_update(handler)
			handler.prm_req["page"] = "update_list"
			handler.prm_cmn["lst_update"] = handler.ctrl_db["db_control"].select("tbl_update")
		super().view(handler)

	def edit_view(self, handler):
		handler.prm_cmn["dict_settings"] = {}
		for record in handler.ctrl_db["db_control"].select("tbl_setting"):
			handler.prm_cmn["dict_settings"][record["setting_key"]] = record["setting_value"]

	def edit_commit(self, handler):
		try:
			handler.ctrl_db["db_control"].begin()
			for setting in self.setting_keys:
				handler.ctrl_db["db_control"].delete("tbl_setting", [{ "setting_key" : setting["key"] }])
				setting_value = handler.prm_req.get(setting["key"], "") or setting["default"]
				handler.ctrl_db["db_control"].insert("tbl_setting", [{
					"setting_key" : setting["key"],
					"setting_value" : setting_value
				}])
			# コミット
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("AC-0002")
			return True
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False
	
	def view_notice_edit(self, handler):
		prm_req = handler.prm_req
		notice_id = prm_req.get("notice_id", "")
		dat_notice = handler.ctrl_db["db_control"].select("tbl_notice", dict_select={
			"id": notice_id
		})
		if len(dat_notice) > 0:
			prm_req["notice_text"] = dat_notice[0]["notice_text"]

	def view_update_edit(self, handler):
		prm_req = handler.prm_req
		update_id = prm_req.get("update_id", "")
		dat_update = handler.ctrl_db["db_control"].select("tbl_update", dict_select={
			"id": update_id
		})
		if len(dat_update) > 0:
			prm_req["update_date"] = dt.strptime(dat_update[0]["update_date"], "%Y/%m/%d　%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S")
			prm_req["update_text"] = dat_update[0]["update_text"]

	def check_notice_edit(self, handler):
		prm_req = handler.prm_req
		notice_text = prm_req.get("notice_text", "")

		result = True
		if UtilCheck.is_empty(notice_text):
			handler.alert_message("CE-0011", ["お知らせ情報"])
			result = False
		return result

	def check_update_edit(self, handler):
		prm_req = handler.prm_req
		page = prm_req.get("page", "user_setting")
		update_date = prm_req.get("update_date", "")
		update_text = prm_req.get("update_text", "")

		result = True
		if page == "update_edit_commit":
			if UtilCheck.is_empty(update_date):
				handler.alert_message("CE-0011", ["更新日時"])
				result = False
		if UtilCheck.is_empty(update_text):
			handler.alert_message("CE-0011", ["更新情報"])
			result = False
		return result

	def add_notice(self, handler):
		prm_req = handler.prm_req
		notice_id = str(uuid.uuid4())

		dat_notice = {
			"id": notice_id,
			"notice_text": prm_req.get("notice_text", "")
		}
		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].insert("tbl_notice", [dat_notice])
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("C-0002", ["お知らせ情報"])
			handler.prm_req["notice_id"] = notice_id
			return True
		except Exception as e:
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def add_update(self, handler):
		prm_req = handler.prm_req
		update_id = str(uuid.uuid4())

		dat_update = {
			"id": update_id,
			"update_date": dt.now().strftime('%Y/%m/%d　%H:%M:%S'),
			"update_text": prm_req.get("update_text", "")
		}
		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].insert("tbl_update", [dat_update])
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("C-0002", ["更新情報"])
			handler.prm_req["update_id"] = update_id
			return True
		except Exception as e:
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def edit_notice(self, handler):
		prm_req = handler.prm_req
		notice_id = prm_req.get("notice_id", "")
		dat_notice = {
			"notice_text": prm_req.get("notice_text", "")
		}
		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].update("tbl_notice", dat_notice, {
				"id": notice_id
			})
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("C-0003", ["お知らせ情報"])
			return True
		except Exception as e:
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def edit_update(self, handler):
		prm_req = handler.prm_req
		update_id = prm_req.get("update_id", "")
		dat_update = {
			"update_date": dt.strptime(prm_req.get("update_date", ""), "%Y-%m-%dT%H:%M:%S").strftime("%Y/%m/%d　%H:%M:%S"),
			"update_text": prm_req.get("update_text", "")
		}
		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].update("tbl_update", dat_update, {
				"id": update_id
			})
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("C-0003", ["更新情報"])
			return True
		except Exception as e:
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def delete_notice(self, handler):
		prm_req = handler.prm_req
		notice_id = prm_req.get("notice_id", "")
		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].delete("tbl_notice", [{"id": notice_id}])
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("C-0004", ["お知らせ情報"])
			return True
		except Exception as e:
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def delete_update(self, handler):
		prm_req = handler.prm_req
		update_id = prm_req.get("update_id", "")
		try:
			handler.ctrl_db["db_control"].begin()
			handler.ctrl_db["db_control"].delete("tbl_update", [{"id": update_id}])
			handler.ctrl_db["db_control"].commit()
			handler.normal_message("C-0004", ["更新情報"])
			return True
		except Exception as e:
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False
