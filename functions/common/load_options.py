# -*- coding: utf-8 -*-

import os
from tornado.options import define, options

class LoadOptions():
	"""
	設定ファイル読込制御クラス
	"""

	@staticmethod
	def define_options():
		"""
		設定定義処理
		"""
		define("debug_mode", default=True, type=bool)
		define("ssl", default=False, type=bool)
		define("certfile", default="")
		define("keyfile", default="")
		define("port", default=80, type=int)
		define("ssl_port", default=443, type=int)
		define("multi_thread", default=False, type=bool)
		define("thread", default=0, type=int)
		define("cookie_key", default="doll")

		define("template_path", default="templates")
		define("static_path", default="static")
		define("define_path", default="define")
		define("db_path", default="db")
		define("site_title", default="i-CMS")
		define("site_author", default="i-tools")

		define("ldap", default=False, type=bool)
		define("ldap_host", default="127.0.0.1")
		define("ldap_port", default=389, type=int)
		define("ldap_domain", default="")
		define("ldap_user", default="")
		define("ldap_password", default="")

		define("host_name", default="127.0.0.1")

		define("ssh_tunnel", default=False, type=bool)
		define("ssh_host", default="127.0.0.1")
		define("ssh_port", default=22, type=int)
		define("ssh_user", default="")
		define("ssh_password", default="")
		define("bind_address", default="127.0.0.1")
		define("remote_bind_port", default=22, type=int)
		define("local_bind_port", default=22, type=int)
		
	@staticmethod
	def load(base_path):
		"""
		設定ファイル読込処理

		Parameters
		----------
		base_path : string
			読込基準パス
		"""
		# 設定ファイル読込
		options.parse_config_file(
			os.path.join(base_path, 'server.ini')
		)
		
		# コマンドライン設定
		options.parse_command_line()

