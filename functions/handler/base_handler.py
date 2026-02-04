# -*- coding: utf-8 -*-

import json
import uuid
import traceback
import threading
import urllib.parse
from datetime import datetime
from collections import OrderedDict
from tornado import web, escape
from tornado.options import options
from tornado.log import app_log
from functions.common.initialize_data import InitializeData
from functions.common.control_db import ControlDB
from functions.common.except_common import ExceptCommon

class BaseHandler(web.RequestHandler):
	"""
	HTTPリクエスト処理基底クラス
	
	Attributes
	----------
	pages : list of functions.page.*.Page
		ユーザ定義Pageクラスの配列
	admin_page : boolean
		本リクエスト処理が管理者用機能であるかどうかを示す。
		True : 管理者用
		False : 一般利用者用
	ctrl_define : functions.common.control_define.ControlDefine
		定義管理クラス
	ctrl_db : functions.common.control_db.ControlDB
		DB管理クラス
	lock : threading.Lock
		排他制御オブジェクト
	prm_cmn : dict
		汎用パラメータ格納辞書
	prm_get : dict
		GETパラメータ格納辞書
	prm_req : dict
		POSTパラメータ格納辞書
	
	Notes
	-----
	新たなリクエスト処理を実装する場合、本クラスを継承して実装すること。
	"""
	
	def initialize(self, pages, ctrl_define):
		"""
		初期化処理
		
		Parameters
		----------
		pages : Pageクラス配列
			ユーザ定義Pageクラスの配列
		ctrl_define : ControlDefine
			定義管理クラス
		"""
		self.pages = pages
		self.ctrl_define = ctrl_define
		self.admin_page = False
		self.lock = threading.Lock()
		
		# 汎用パラメータ
		self.prm_cmn = OrderedDict()
		# GETパラメータ
		self.prm_get = OrderedDict()

	def prepare(self):
		"""
		リクエスト前処理
		"""
		# DB制御
		self.ctrl_db = ControlDB(self.ctrl_define)

		# 基本設定読込
		self.prm_cmn["use_ssl"] = options.ssl
		if not options.ssl:
			self.prm_cmn["http_host"] = "http://{0}:{1}".format(options.host_name, options.port)
			self.prm_cmn["websocket_host"] = "ws://{0}:{1}".format(options.host_name, options.port)
		else:
			self.prm_cmn["http_host"] = "https://{0}:{1}".format(options.host_name, options.ssl_port)
			self.prm_cmn["websocket_host"] = "wss://{0}:{1}".format(options.host_name, options.ssl_port)
		self.prm_cmn["use_ldap"] = options.ldap
		self.prm_cmn["color_role"] = {}
		self.prm_cmn["message"] = OrderedDict()
		self.prm_cmn["lst_breadcrumb"] = []
		for lv in self.ctrl_define["message_lv"]["def"].keys():
			self.prm_cmn["color_role"][lv] = self.ctrl_define["message_lv"]["def"][lv].get("value", "")
			self.prm_cmn["message"][lv] = []
			
		self.is_error = False
		try:
			for param in self.request.query.split("&"):
				if param == "":
					break
				key = param.split("=")
				self.prm_get[key[0]] = self.get_query_argument(key[0])
			# リクエストパラメータ
			self.prm_req = OrderedDict()
			for key in self.request.arguments.keys():
				self.prm_req[key] = self.get_argument(key)
			self.prm_file = OrderedDict()
			for key in self.request.files.keys():
				self.prm_file[key] = self.request.files[key]
			
			# 初期設定の要否確認
			self.lock.acquire()
			tbl_setting = self.ctrl_db["db_control"].select("tbl_setting")
			if len(tbl_setting) == 0:
				InitializeData.exec(self)
			self.lock.release()

			for record in self.ctrl_db["db_control"].select("tbl_setting"):
				self.prm_cmn[record["setting_key"]] = record["setting_value"]
		except Exception as e:
			self.append_log(self.prm_cmn.get("account_id", "Non Login"), "alert")
			print(e)
			self.append_message("CE-9999", [str(e)])
			for m in str(traceback.format_exc()).splitlines():
				self.append_message("", [m], "alert")
			self.is_error = True
	
	def on_finish(self):
		"""
		リクエスト後処理
		"""
		# DBクローズ
		self.ctrl_db.__del__()

	def get(self):
		"""
		GETリクエスト処理
		"""
		self.proc_page("get")

	def post(self):
		"""
		POSTリクエスト処理
		"""
		self.proc_page("post")
		
	def put(self):
		"""
		PUTリクエスト処理
		"""
		self.proc_page("put")

	def delete(self):
		"""
		DELETEリクエスト処理
		"""
		self.proc_page("delete")
	
	def proc_page(self, mode):
		try:
			if self.is_error:
				self.render("base.html", prm_cmn=self.prm_cmn)
				return
			operation = self.get_operation()
			if len(operation) > 0:
				self.prm_req["page"] = operation[0]["return_operation"]
			key = self.prm_get.get("page", None)
			if key is None:
				key = self.prm_req.get("page", "index")
			for page in self.pages:
				if key in page.page_handler:
					if self.prm_cmn.get("maintenance", "0") == "1" and not page.maintenance_use:
						if not page.check_login(self) or not self.prm_cmn.get("admin", False):
							self.render("common/maintenance.html", prm_cmn=self.prm_cmn, prm_req=self.prm_req)
							return
					if page.need_login:
						if not self.admin_page and not page.check_login(self):
							self.append_message("CE-0003", [key])
							self.prm_req["page"] = page.back_page
							self.proc_page(mode)
							return
						self.append_access_hist()
						if self.admin_page and not page.admin_check_login(self):
							self.append_message("CE-0004", [key])
							self.prm_req["page"] = page.back_page
							self.proc_page(mode)
							return
					if page.need_auth != "" and not self.prm_cmn.get("auth", {}).get(page.need_auth, False):
						self.append_message("CE-0005", [key])
						self.prm_req["page"] = page.back_page_auth
						self.proc_page(mode)
						return
					if mode == "get":
						page.get_view(self)
					elif mode == "post":
						page.post_view(self)
					elif mode == "put":
						page.put_view(self)
					elif mode == "delete":
						page.delete_view(self)
					return
			self.append_message("CE-0006", [key])
			self.render("common/error.html", prm_cmn=self.prm_cmn, prm_req=self.prm_req)
		except ExceptCommon as e:
			self.append_message(e.msg_id, e.param)
			self.render("base.html", prm_cmn=self.prm_cmn)
		except Exception as e:
			self.append_log(self.prm_cmn.get("account_id", "Non Login"), "alert")
			print(e)
			#print(traceback.format_exc())
			self.append_message("CE-9999", [str(e)])
			for m in str(traceback.format_exc()).splitlines():
				self.append_message("", [m], "alert")
			self.render("base.html", prm_cmn=self.prm_cmn)

	def view_download(self, file_name, file_io):
		"""
		ファイルダウンロード応答処理
		
		Parameters
		----------
		file_name : string
			ダウンロードファイル名
		file_io : FileIO
			ダウンロード対象のファイルIOオブジェクト
		"""
		self.set_header("Content-Type", "application/octet-stream")
		quote_name = urllib.parse.quote(file_name)
		self.set_header("Content-Disposition", f"attachment; filename={quote_name}; filename*=UTF-8'{quote_name}'")
		buf_size = 4096
		while True:
			data = file_io.read(buf_size)
			if not data:
				break
			self.write(data)
		self.finish()
	
	def view_json(self, resonse):
		"""
		JSON応答処理
		
		Parameters
		----------
		response : dict
			応答電文
		"""
		self.set_header("Content-Type", "application/json")
		self.write(json.dumps(resonse))
		self.finish()

	def add_breadcrumb(self, title, link=None, icon=None, param={}):
		self.prm_cmn["lst_breadcrumb"].append({
			"link": link,
			"icon": icon,
			"title": title,
			"param": param
		})

	def get_cookie_value(self, key):
		"""
		cookie取得処理
		
		Parameters
		----------
		key : string
			cookieのキー値
		"""
		value = self.get_signed_cookie(key)
		if not value: return None
		return escape.xhtml_unescape(value)

	def set_cookie_value(self, key, value):
		"""
		cookie設定処理
		
		Parameters
		----------
		key : string
			cookieのキー値
		value : string
			cookieの設定値
		"""
		self.set_signed_cookie(key, escape.xhtml_escape(value))
	
	def alert_message(self, msg_cd, msg_prm=[]):
		"""
		画面メッセージ出力処理(Alert)
		
		Parameters
		----------
		msg_cd : string
			出力するメッセージID。
		msg_prm : list of string ,default []
			メッセージに埋め込むパラメータ配列。
		"""
		self.append_message(msg_cd, msg_prm, "alert")
	
	def warning_message(self, msg_cd, msg_prm=[]):
		"""
		画面メッセージ出力処理(Warning)
		
		Parameters
		----------
		msg_cd : string
			出力するメッセージID。
		msg_prm : list of string ,default []
			メッセージに埋め込むパラメータ配列。
		"""
		self.append_message(msg_cd, msg_prm, "warning")
	
	def normal_message(self, msg_cd, msg_prm=[]):
		"""
		画面メッセージ出力処理(Normal)
		
		Parameters
		----------
		msg_cd : string
			出力するメッセージID。
		msg_prm : list of string ,default []
			メッセージに埋め込むパラメータ配列。
		"""
		self.append_message(msg_cd, msg_prm, "normal")
	
	def append_message(self, msg_cd, msg_prm=[], msg_type=""):
		"""
		画面メッセージ出力処理
		
		Parameters
		----------
		msg_cd : string
			出力するメッセージID。
		msg_prm : list of string ,default []
			メッセージに埋め込むパラメータ配列。msg_cdが未設定の場合は本パラメータに設定された項目を全て出力。
		msg_type : string, default ""
			出力するメッセージの種別。未設定の場合、メッセージ定義に設定されたメッセージレベルを使用する。
		"""
		if not msg_type in self.prm_cmn["message"].keys():
			self.prm_cmn["color_role"][msg_type] = ""
			self.prm_cmn["message"][msg_type] = []
		if msg_cd != "":
			def_message = self.ctrl_define["message"]["def"].get(msg_cd, {})
			if msg_type == "":
				msg_type = def_message.get("lv", "normal")
			message = "[{0}] {1}".format(msg_cd, def_message.get("text", "メッセージ未定義").format(msg_prm))
			self.prm_cmn["message"][msg_type].append(message)
			self.append_log(message, msg_type)
			return message
		else:
			for m in msg_prm:
				self.prm_cmn["message"][msg_type].append(m)
				self.append_log(m, msg_type)
		return "\n".join(self.prm_cmn["message"][msg_type])

	def append_log(self, message, msg_type="normal"):
		"""
		APPログ出力処理
		
		Parameters
		----------
		message : string
			出力するログメッセージ。
		msg_type : string, default normal
			ログレベル(alert,warning,normal,debug)。
		"""
		if msg_type == "alert":
			app_log.error(message)
		elif msg_type == "warning":
			app_log.warning(message)
		elif msg_type == "normal":
			app_log.info(message)
		else:
			app_log.debug(message)

	def append_access_hist(self):
		"""
		操作履歴追加処理
		
		Parameters
		----------
		op : string
			操作名
		return_op : string
			表示操作
		"""
		access_data = {
			"access_date": datetime.now().strftime("%Y-%m-%d"),
			"account_id": self.prm_cmn["account_id"]
		}
		if len(self.ctrl_db["db_control"].select("tbl_access_hist", access_data)) > 0:
			return
		self.ctrl_db["db_control"].begin()
		self.ctrl_db["db_control"].insert("tbl_access_hist", [access_data])
		self.ctrl_db["db_control"].commit()

	def get_operation_id(self):
		"""
		操作ID取得処理
		"""
		operation_id = self.prm_req.get("_xsrf")
		if operation_id is None:
			operation_id = uuid.uuid4().hex
		else:
			operation_id = operation_id.split("|")[2]
		return operation_id

	def get_operation(self):
		"""
		操作履歴取得処理
		"""
		operation_id = self.get_operation_id()
		return self.ctrl_db["db_control"].select("tbl_operation_hist", { "operation_id": operation_id })

	def append_operation(self, op, return_op):
		"""
		操作履歴追加処理
		
		Parameters
		----------
		op : string
			操作名
		return_op : string
			表示操作
		"""
		self.ctrl_db["db_control"].begin()
		operation_id = self.get_operation_id()
		operation_data = {
			"operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
			"operation_id": operation_id,
			"account_id": self.prm_cmn["account_id"],
			"operation": op,
			"return_operation": return_op,
			"args": json.dumps(self.prm_req)
		}
		self.ctrl_db["db_control"].insert("tbl_operation_hist", [operation_data])
		self.ctrl_db["db_control"].commit()

	def get_auth_list(self):
		result = {}
		for record in self.ctrl_db["db_control"].select("mst_auth"):
			result[record["function"]] = {
				"id": record["function"],
				"name": record["auth_name"],
				"ref_name": record["auth_name"]
			}
		return result