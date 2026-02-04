# -*- coding: utf-8 -*-

import re
import uuid
import traceback
from functions.page.base_page import BasePage
from tornado.options import options
from functions.define.menu_define import MenuDefine

class Page(BasePage):
	def __init__(self):
		super().__init__()
		self.page_handler.extend([
			"group_setting", 
			"group_setting_add", 
			"group_setting_add_commit", 
			"group_setting_edit", 
			"group_setting_edit_commit", 
			"group_setting_delete",
			"group_affiliation_delete"
		])
		# MENUID, TITLE, ACTION, DISABLE, ADMIN, AUTH, ICON, ORDER, T_COLOR, B_COLOR
		self.portal_menu = MenuDefine("MA012", "グループ管理", "group_setting", False, True, None, "group", 12, "white-text", "light-green darken-1")
		# ログイン状態である必要があるか
		self.need_login = True
		# need_login=Trueでログインしていなかった場合に表示されるページ
		self.back_page = "index"
		# templateファイルの格納ディレクトリ
		self.page_dir = "group_setting"

		self.input_check = re.compile(r'^[a-zA-Z0-9\-_]+$')

	def post_view(self, handler):
		handler.prm_cmn["define_auth"] = handler.ctrl_define["auth"]["def"]
		page = handler.prm_req.get("page", "group_setting")
		if page == "group_setting":
			handler.prm_cmn["lst_group"] = self.get_groups(handler)
		elif page == "group_setting_add":
			self.edit_view(handler)
		elif page == "group_setting_add_commit":
			if self.add_commit(handler):
				handler.prm_req["page"] = "group_setting_edit"
			else:
				handler.prm_req["page"] = "group_setting_add"
			self.edit_view(handler)
		elif page == "group_setting_edit":
			self.edit_view(handler)
		elif page == "group_setting_edit_commit":
			self.edit_commit(handler)
			handler.prm_req["page"] = "group_setting_edit"
			self.edit_view(handler)
		elif page == "group_setting_delete":
			self.delete_commit(handler)
		elif page == "group_affiliation_delete":
			self.delete_affiliation(handler)
			handler.prm_req["page"] = "group_setting_edit"
			self.edit_view(handler)

		super().view(handler)

	def get_groups(self, handler):
		result = []
		groups = []
		for record in handler.ctrl_db["db_control"].select("tbl_group"):
			group = {
				"id": record["id"],
				"name": record["name"],
				"manage_system": record["manage_system"],
				"manage_sales": record["manage_sales"],
				"auth": {}
			}
			for auth in handler.ctrl_db["db_control"].select("tbl_group_auth", dict_select={ "id" : record["id"] }):
				if auth["auth_value"]:
					group["auth"][auth["function"]] = auth["auth_value"]
			group["num"] = len(handler.ctrl_db["db_control"].select("tbl_group_affiliation", dict_select={ "group_id" : record["id"] }))
			groups.append(record["id"])
			result.append(group)
		dict_auth = {}
		for record in handler.ctrl_db["db_control"].select("tbl_group_auth"):
			if record["id"] in groups:
				continue
			if not record["id"] in dict_auth.keys():
				dict_auth[record["id"]] = {}
			if record["auth_value"]:
				dict_auth[record["id"]][record["function"]] = record["auth_value"]
		
		result = sorted(result, key=lambda x: x["id"])
		
		return result

	def edit_view(self, handler):
		handler.prm_cmn["info_groups"] = {}

		group_id = ""
		if handler.prm_req.get("page", "group_setting") == "group_setting_edit":
			group_id = handler.prm_req.get("target_group_id", handler.prm_req.get("group_id", ""))
			dat_group = handler.ctrl_db["db_control"].select("tbl_group", dict_select = { "id" : group_id })
			if len(dat_group) > 0:
				handler.prm_req["group_name"] = dat_group[0]["name"]
				handler.prm_req["manage_system"] = dat_group[0]["manage_system"]
				handler.prm_req["manage_sales"] = dat_group[0]["manage_sales"]
			
			for auth in handler.ctrl_db["db_control"].select("tbl_group_auth", dict_select = { "id" : group_id }):
				if auth["auth_value"]:
					handler.prm_req["chk_{0}".format(auth["function"])] = "1"
		for record in self.get_groups(handler):
			if group_id != record["id"] and record["id"] in handler.prm_cmn["info_groups"].keys():
				del handler.prm_cmn["info_groups"][record["id"]]

		handler.prm_cmn["lst_group_affiliation"] = []
		for grp in handler.ctrl_db["db_control"].select("tbl_group_affiliation", dict_select = { "group_id" : group_id }):
			handler.prm_cmn["lst_group_affiliation"].append({
				"id": grp["account_id"],
				"name": grp["account_name"]
			})

	def add_commit(self, handler):
		group_id = str(uuid.uuid4())
		group_name = handler.prm_req.get("group_name", "")
		chk_auth = {}
		for key in handler.prm_cmn["define_auth"].keys():
			chk_auth[key] = handler.prm_req.get("chk_{0}".format(key), "0")
		
		# 入力チェック
		flg = False
		if group_name == "":
			handler.alert_message("ACE-0004")
			flg = True
		if flg:
			return False

		try:
			handler.ctrl_db["db_control"].begin()
			group_data = {
				"id": group_id,
				"name": group_name,
				"manage_system": handler.prm_req.get("manage_system", "0"),
				"manage_sales": handler.prm_req.get("manage_sales", "0")
			}
			handler.ctrl_db["db_control"].insert("tbl_group", [group_data])
			handler.ctrl_db["db_control"].delete("tbl_group_auth", [{ "id" : group_id }])
			insert_data = []
			for key in chk_auth.keys():
				record = {
					"id": group_id,
					"function": key,
					"auth_value": chk_auth[key] == "1"
				}
				insert_data.append(record)
			handler.ctrl_db["db_control"].insert("tbl_group_auth", insert_data)

			# コミット
			handler.ctrl_db["db_control"].commit()

			handler.normal_message("AC-0001")
			return True
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def edit_commit(self, handler):
		group_id = handler.prm_req.get("group_id", "")
		group_name = handler.prm_req.get("group_name", "")
		chk_auth = {}
		for key in handler.prm_cmn["define_auth"].keys():
			chk_auth[key] = handler.prm_req.get("chk_{0}".format(key), "0")
		
		# 入力チェック
		flg = False
		if group_name == "":
			handler.alert_message("ACE-0004")
			flg = True
		if flg:
			return False

		try:
			handler.ctrl_db["db_control"].begin()
			# 独自ユーザ登録
			group_data = {
				"name": group_name,
				"manage_system": handler.prm_req.get("manage_system", "0"),
				"manage_sales": handler.prm_req.get("manage_sales", "0")
			}
			handler.ctrl_db["db_control"].update("tbl_group", group_data, { "id" : group_id })
			handler.ctrl_db["db_control"].delete("tbl_group_auth", [{ "id" : group_id }])
			insert_data = []
			for key in chk_auth.keys():
				record = {
					"id": group_id,
					"function": key,
					"auth_value": chk_auth[key] == "1"
				}
				insert_data.append(record)
			handler.ctrl_db["db_control"].insert("tbl_group_auth", insert_data)

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

	def delete_commit(self, handler):
		try:
			handler.ctrl_db["db_control"].begin()
			group_id = handler.prm_req.get("group_id", "")

			handler.ctrl_db["db_control"].delete("tbl_group", [{ "id" : group_id }])
			handler.ctrl_db["db_control"].delete("tbl_group_auth", [{ "id" : group_id }])

			# コミット
			handler.ctrl_db["db_control"].commit()

			handler.normal_message("AC-0003")
			handler.prm_req["page"] = "group_setting"
			handler.prm_cmn["lst_group"] = self.get_groups(handler)
			return True
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def delete_affiliation(self, handler):
		try:
			handler.ctrl_db["db_control"].begin()
			group_id = handler.prm_req.get("group_id", "")
			user_id = handler.prm_req.get("user_id", "")

			handler.ctrl_db["db_control"].delete("tbl_group_affiliation", [{ "group_id" : group_id, "account_id" : user_id }])

			# コミット
			handler.ctrl_db["db_control"].commit()

			handler.normal_message("AC-0002")
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
