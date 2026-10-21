# -*- coding: utf-8 -*-

import uuid
import traceback
from datetime import datetime, timedelta
from tornado.options import options
from functions.common.util_encrypt import UtilEncrypt

class InitializeData():
	"""
	i-CMS管理データ初期化クラス
	
	Notes
	-----
	本クラスの関数は静的関数となるため、クラスオブジェクトの生成を行わずに使用すること。
	"""

	@staticmethod
	def exec(handler):
		"""
		初期化処理

		Parameters
		----------
		handler : functions.handler.base_handler.BaseHandler
			HTTPリクエスト処理クラス
		"""
		try:
			handler.ctrl_db["db_control"].begin()
			# 初期化
			for db in handler.ctrl_db.keys():
				for tbl in handler.ctrl_db[db].tables.keys():
					handler.ctrl_db[db].drop_table(tbl)
					handler.ctrl_db[db].create_table(tbl)

			# 基本設定
			handler.ctrl_db["db_control"].insert("tbl_setting", [
				{
					"setting_key": "title",
					"setting_value": options.site_title
				},
				{
					"setting_key": "author",
					"setting_value": options.site_author
				},
				{
					"setting_key": "description",
					"setting_value": ""
				},
				{
					"setting_key": "keywords",
					"setting_value": ""
				},
				{
					"setting_key": "maintenance",
					"setting_value": False
				},
				{
					"setting_key": "maintenance_notice",
					"setting_value": ""
				},
				{
					"setting_key": "maintenance_enddate",
					"setting_value": ""
				}
			])

			# グループ設定
			grp_id = str(uuid.uuid4())
			handler.ctrl_db["db_control"].insert("tbl_group", [{
				"id": grp_id,
				"name": "システム管理グループ",
				"admin": True
			}])
			
			# 権限設定
			insert_data = []
			for auth in handler.ctrl_define["auth"]["def"].values():
				insert_data.append({
					"id": auth.get("id"),
					"name": auth.get("name"),
					"function": InitializeData.get_auth_function(handler, auth.get("id")),
					"operation": auth.get("operation"),
				})
			handler.ctrl_db["db_control"].insert("mst_auth", insert_data)
			user = handler.ctrl_define["user"]["def"]
			insert_data = []
			updated_at = datetime.now().isoformat()
			for idx, user_id in enumerate(user.keys()):
				user_data = {
					"id": idx,
					"user_id": user_id,
					"name": user_id,
					"email": "",
					"admin": False,
					"is_active": True,
					"is_password_set": False,
					"created_at": updated_at,
					"created_by": idx,
					"updated_at": updated_at,
					"updated_by": idx,
				}
				for auth_key in user[user_id].keys():
					record = {
						"id": idx,
						"created_at": updated_at,
						"created_by": idx,
						"updated_at": updated_at,
						"updated_by": idx,
					}
					if auth_key == "id":
						continue
					if auth_key == "password":
						if user[user_id][auth_key] != "":
							user_data["password"] = UtilEncrypt.hash_password(user[user_id][auth_key])
						continue
					if auth_key == "name":
						user_data["name"] = user[user_id][auth_key]
						continue
					if auth_key == "admin":
						user_data["admin"] = user[user_id][auth_key] == "1"
						continue
					# ユーザ定義の属性名は権限定義のIDのため、mst_auth.functionと同じ値（ref_name）へ変換する
					# （tbl_authはfunctionでmst_authと結合するため、IDのままでは権限が取得できない）
					record["function"] = InitializeData.get_auth_function(handler, auth_key)
					record["auth_value"] = user[user_id][auth_key] in ["1", "〇"]
					insert_data.append(record)
				if "password" in user_data.keys():
					handler.ctrl_db["db_control"].insert("tbl_account", [user_data])
					if user_data["admin"]:
						handler.ctrl_db["db_control"].insert("tbl_group_affiliation", [{
							"group_id": grp_id,
							"account_id": user_data["id"]
						}])
			handler.ctrl_db["db_control"].insert("tbl_auth", insert_data)

			# コミット
			handler.ctrl_db["db_control"].commit()
		except Exception as e:
			# ロールバック
			handler.ctrl_db["db_control"].rollback()
			print(e)
			print(traceback.format_exc())
			handler.append_message("CE-9999", [str(e)])

	@staticmethod
	def get_auth_function(handler, auth_id):
		"""
		権限定義のIDに対応する機能名（mst_auth.function）の取得処理

		Parameters
		----------
		handler : functions.handler.base_handler.BaseHandler
			HTTPリクエスト処理クラス
		auth_id : string
			権限定義（define/common/auth.xml）のID

		Returns
		-------
		string
			権限定義のref_name。権限定義に存在しないIDの場合はauth_idをそのまま返す
		"""
		auth = handler.ctrl_define["auth"]["def"].get(auth_id)
		if auth is None or not auth.get("ref_name"):
			return auth_id
		return auth.get("ref_name")
