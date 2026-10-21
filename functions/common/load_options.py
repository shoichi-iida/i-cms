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
		define("cookie_key", default="i-cms", help="Cookieの暗号化キー")
		define("secrets_key", default="12345678901234567890123456789012", help="JWTの暗号化キー")

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
		define("frontend_url", default="http://127.0.0.1:5173")
		define("encrypt_key", default="i-cms")
		# bcryptのコストファクタ（パスワードのハッシュ化）。
		# 値を下げると総当たり耐性が落ちるため、本番設定では下げないこと
		define("bcrypt_rounds", default=12, type=int)

		define("ssh_tunnel", default=False, type=bool)
		define("ssh_host", default="127.0.0.1")
		define("ssh_port", default=22, type=int)
		define("ssh_user", default="")
		define("ssh_password", default="")
		define("bind_address", default="127.0.0.1")
		define("remote_bind_port", default=[22], type=int, multiple=True)
		define("local_bind_port", default=[22], type=int, multiple=True)
		
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

		LoadOptions.validate()

	@staticmethod
	def validate():
		"""
		設定値の検証処理

		決まった値の中から選ぶ設定が、決まった値かを確かめる。誤っていれば
		ValueErrorを送出し、起動を止める（APIの実行時に気づくのでは遅いため）。
		"""
		choices = (
		)
		for name, values in choices:
			if options[name] not in values:
				raise ValueError("server.iniの{0}は{1}のいずれかを指定してください（指定値: {2!r}）".format(
					name, "・".join(values), options[name]))

