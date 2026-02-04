# -*- coding: utf-8 -*-

import re
import traceback
from functions.page.base_page import BasePage
from tornado.options import options
from functions.define.menu_define import MenuDefine
from functions.common.util_check import UtilCheck
if options.ldap:
	from functions.common.control_ldap import ControlLdap

class Page(BasePage):
	def __init__(self):
		super().__init__()
		self.page_handler.extend([
			"user_setting", 
			"user_setting_add", 
			"user_setting_add_commit", 
			"user_setting_edit", 
			"user_setting_edit_commit", 
			"user_setting_delete"
		])
		# MENUID, TITLE, ACTION, DISABLE, ADMIN, AUTH, ICON, ORDER, T_COLOR, B_COLOR
		self.portal_menu = MenuDefine("MA011", "ユーザ管理", "user_setting", False, True, None, "account_box", 11, "white-text", "light-green darken-1")
		# ログイン状態である必要があるか
		self.need_login = True
		# need_login=Trueでログインしていなかった場合に表示されるページ
		self.back_page = "index"
		# templateファイルの格納ディレクトリ
		self.page_dir = "user_setting"

		self.input_check = re.compile(r'^[a-zA-Z0-9\-_]+$')
		if options.ldap:
			self.ldap = ControlLdap()
			self.ldap.connect(options.ldap_domain, options.ldap_user, options.ldap_password)

	def post_view(self, handler):
		handler.prm_cmn["define_auth"] = handler.ctrl_define["auth"]["def"]
		page = handler.prm_req.get("page", "user_setting")
		if page == "user_setting":
			handler.prm_cmn["lst_user"] = self.get_users(handler)
		elif page == "user_setting_add":
			self.edit_view(handler)
		elif page == "user_setting_add_commit":
			if self.add_commit(handler):
				handler.prm_req["page"] = "user_setting_edit"
			else:
				handler.prm_req["page"] = "user_setting_add"
			self.edit_view(handler)
		elif page == "user_setting_edit":
			self.edit_view(handler)
		elif page == "user_setting_edit_commit":
			self.edit_commit(handler)
			handler.prm_req["page"] = "user_setting_edit"
			self.edit_view(handler)
		elif page == "user_setting_delete":
			self.delete_commit(handler)

		super().view(handler)

	def get_users(self, handler):
		result = []
		users = []
		for record in handler.ctrl_db["db_control"].select("tbl_account"):
			user = {
				"id": record["id"],
				"name": record["name"],
				"admin": record["admin"],
				"auth": {}
			}
			for auth in handler.ctrl_db["db_control"].select("tbl_auth", dict_select={ "id" : record["id"] }):
				if auth["auth_value"]:
					user["auth"][auth["function"]] = auth["auth_value"]
			users.append(record["id"])
			result.append(user)
		dict_auth = {}
		for record in handler.ctrl_db["db_control"].select("tbl_auth"):
			if record["id"] in users:
				continue
			if not record["id"] in dict_auth.keys():
				dict_auth[record["id"]] = {}
			if record["auth_value"]:
				dict_auth[record["id"]][record["function"]] = record["auth_value"]
		if options.ldap:
			info_users = self.ldap.get_user_names()
			for key in dict_auth.keys():
				result.append({
					"id": key,
					"name": info_users.get(key, {}).get("user_name", key),
					"admin": False,
					"auth": dict_auth[key]
				})
		
		result = sorted(result, key=lambda x: x["id"])
		
		return result

	def edit_view(self, handler):
		if options.ldap:
			handler.prm_cmn["info_users"] = self.ldap.get_user_names()
		else:
			handler.prm_cmn["info_users"] = {}

		user_id = ""
		if handler.prm_req.get("page", "user_setting") == "user_setting_edit":
			user_id = handler.prm_req.get("target_user_id", handler.prm_req.get("user_id", ""))
			dat_account = handler.ctrl_db["db_control"].select("tbl_account", dict_select = { "id" : user_id })
			if len(dat_account) == 0:
				handler.prm_req["user_type"] = "1"
				handler.prm_req["user_id"] = ""
				handler.prm_req["user_id_select"] = user_id
			else:
				handler.prm_req["user_type"] = "0"
				handler.prm_req["user_password"] = dat_account[0]["password"]
				handler.prm_req["user_name"] = dat_account[0]["name"]
				if dat_account[0]["admin"]:
					handler.prm_req["role_type"] = "1"
			for auth in handler.ctrl_db["db_control"].select("tbl_auth", dict_select = { "id" : user_id }):
				if auth["auth_value"]:
					handler.prm_req["chk_{0}".format(auth["function"])] = "1"
			
			handler.prm_req["lst_group_affiliation"] = []
			for grp in handler.ctrl_db["db_control"].select("tbl_group_affiliation", dict_select = { "account_id" : user_id }):
				handler.prm_req["lst_group_affiliation"].append(grp["group_id"])

		for record in self.get_users(handler):
			if user_id != record["id"] and record["id"] in handler.prm_cmn["info_users"].keys():
				del handler.prm_cmn["info_users"][record["id"]]
		
		handler.prm_cmn["lst_group"] = handler.ctrl_db["db_control"].select("tbl_group")


	def add_commit(self, handler):
		user_type = handler.prm_req.get("user_type", "0")
		user_id = handler.prm_req.get("user_id", "")
		user_password = handler.prm_req.get("user_password", "")
		user_name = handler.prm_req.get("user_name", "")
		user_id_select = handler.prm_req.get("user_id_select", "")
		role_type = handler.prm_req.get("role_type", "0")
		handler.prm_req["lst_group_affiliation"] = handler.prm_req.get("group_affiliation", "").split(",")
		chk_auth = {}
		for key in handler.prm_cmn["define_auth"].keys():
			chk_auth[key] = handler.prm_req.get("chk_{0}".format(key), "0")
		
		# 入力チェック
		flg = False
		if user_type == "0":
			if UtilCheck.is_empty(user_id):
				handler.alert_message("ACE-0001")
				flg = True
			if not UtilCheck.is_empty(user_id) and not self.input_check.match(user_id):
				handler.alert_message("ACE-0002")
				flg = True
			if UtilCheck.is_empty(user_password):
				handler.alert_message("ACE-0003")
				flg = True
			if UtilCheck.is_empty(user_name):
				handler.alert_message("ACE-0004")
				flg = True
		else:
			if UtilCheck.is_empty(user_id_select):
				handler.alert_message("ACE-0005")
				flg = True
		if flg:
			return False

		try:
			handler.ctrl_db["db_control"].begin()
			# 独自ユーザ登録
			if user_type == "0":
				user_data = {
					"id": user_id,
					"name": user_name,
					"password": user_password,
					"admin": role_type == "1"
				}
				handler.ctrl_db["db_control"].insert("tbl_account", [user_data])
			else:
				user_id = user_id_select
			handler.ctrl_db["db_control"].delete("tbl_auth", [{ "id" : user_id }])
			insert_data = []
			for key in chk_auth.keys():
				record = {
					"id": user_id,
					"function": key,
					"auth_value": chk_auth[key] == "1"
				}
				insert_data.append(record)
			handler.ctrl_db["db_control"].insert("tbl_auth", insert_data)

			# 所属
			insert_data = []
			for grp in handler.prm_req["lst_group_affiliation"]:
				if UtilCheck.is_empty(grp):
					continue
				record = {
					"group_id": grp,
					"account_id": user_id
				}
				insert_data.append(record)
			handler.ctrl_db["db_control"].insert("tbl_group_affiliation", insert_data)

			# コミット
			handler.ctrl_db["db_control"].commit()

			handler.normal_message("AC-0001")
			handler.prm_req["target_user_id"] = user_id
			return True
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

	def edit_commit(self, handler):
		target_user_id = handler.prm_req.get("target_user_id", "")
		user_type = handler.prm_req.get("user_type", "0")
		user_id = handler.prm_req.get("user_id", "")
		user_password = handler.prm_req.get("user_password", "")
		user_name = handler.prm_req.get("user_name", "")
		user_id_select = handler.prm_req.get("user_id_select", "")
		role_type = handler.prm_req.get("role_type", "0")
		if not UtilCheck.is_empty(handler.prm_req.get("group_affiliation", "")):
			handler.prm_req["lst_group_affiliation"] = handler.prm_req.get("group_affiliation", "").split(",")
		else:
			handler.prm_req["lst_group_affiliation"] = []
		chk_auth = {}
		for key in handler.prm_cmn["define_auth"].keys():
			chk_auth[key] = handler.prm_req.get("chk_{0}".format(key), "0")
		
		# 入力チェック
		flg = False
		if user_type == "0":
			if UtilCheck.is_empty(user_id):
				handler.alert_message("ACE-0001")
				flg = True
			if not UtilCheck.is_empty(user_id) and not self.input_check.match(user_id):
				handler.alert_message("ACE-0002")
				flg = True
			if UtilCheck.is_empty(user_password):
				handler.alert_message("ACE-0003")
				flg = True
			if UtilCheck.is_empty(user_name):
				handler.alert_message("ACE-0004")
				flg = True
		else:
			if UtilCheck.is_empty(user_id_select):
				handler.alert_message("ACE-0005")
				flg = True
		if flg:
			return False

		try:
			handler.ctrl_db["db_control"].begin()
			# 独自ユーザ登録
			handler.ctrl_db["db_control"].delete("tbl_account", [{ "id" : target_user_id }])
			if user_type == "0":
				user_data = {
					"id": user_id,
					"name": user_name,
					"password": user_password,
					"admin": role_type == "1"
				}
				handler.ctrl_db["db_control"].insert("tbl_account", [user_data])
			else:
				user_id = user_id_select
			handler.ctrl_db["db_control"].delete("tbl_auth", [{ "id" : target_user_id }])
			insert_data = []
			for key in chk_auth.keys():
				record = {
					"id": user_id,
					"function": key,
					"auth_value": chk_auth[key] == "1"
				}
				insert_data.append(record)
			handler.ctrl_db["db_control"].insert("tbl_auth", insert_data)

			# 所属
			handler.ctrl_db["db_control"].delete("tbl_group_affiliation", [{ "account_id" : target_user_id }])
			insert_data = []
			for grp in handler.prm_req["lst_group_affiliation"]:
				record = {
					"group_id": grp,
					"account_id": user_id
				}
				insert_data.append(record)
			handler.ctrl_db["db_control"].insert("tbl_group_affiliation", insert_data)

			# コミット
			handler.ctrl_db["db_control"].commit()

			handler.normal_message("AC-0002")
			handler.prm_req["target_user_id"] = user_id
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
			user_id = handler.prm_req.get("target_user_id", "")

			handler.ctrl_db["db_control"].delete("tbl_account", [{ "id" : user_id }])
			handler.ctrl_db["db_control"].delete("tbl_auth", [{ "id" : user_id }])
			# 所属
			handler.ctrl_db["db_control"].delete("tbl_group_affiliation", [{ "account_id" : user_id }])

			# コミット
			handler.ctrl_db["db_control"].commit()

			handler.normal_message("AC-0003")
			handler.prm_req["page"] = "user_setting"
			handler.prm_cmn["lst_user"] = self.get_users(handler)
			return True
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])
		return False

