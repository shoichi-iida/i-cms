# -*- coding: utf-8 -*-

import os
import shutil
from datetime import datetime
from tinydb.operations import delete
from functions.common.util_check import UtilCheck
from functions.common.util_encrypt import UtilEncrypt
from functions.data.control_sqlite import ControlSqlite
from functions.data.control_tinydb import ControlTinyDB

class MigrateData():
	"""
	既存データ移行クラス

	テーブル定義（define/tables）の変更前に作成されたDBを、現在の定義で使えるようにする。
	初期化（InitializeData）はtbl_settingが空の場合にしか行われないため、
	運用中のDBにはテーブル定義の変更が反映されない。本クラスで次の移行を行う。

	1. 定義に追加された列の追加（SQLite・MySQL。TinyDBは列の概念がないため不要）。
	   MySQLでtbl_account.passwordが短い文字列型（旧setup.sqlのVARCHAR(30)）の場合は拡張
	2. 旧setup.sqlのmst_authの列名の変更（auth_id → id、auth_name → name）と、
	   tbl_authの権限キーの変換（旧: auth_id = mst_auth.id → 新: function = mst_auth.function）
	3. 旧setup.sqlのtbl_account_token（token VARCHAR(50)・expires_at DATE・主キー(id, token)）の
	   作り直し（既存のログインセッションは破棄する）。TinyDBはアカウントIDを文字列にそろえる
	4. tbl_accountの追加列の補完（is_password_set・login_failed_count・作成/更新日時等）
	5. パスワードの移行（XOR暗号 → bcrypt-sha256）

	Notes
	-----
	- 何度実行しても同じ結果になる（移行済みのデータは変更しない）。
	- 変更する前にバックアップを取る（SQLite・MySQL: 「{テーブル名}_bak_{日時}」テーブル、
	  TinyDB: 「{DBファイル}.bak_{日時}」ファイル）。列の追加は既存の値を変えないため取らない。
	- パスワードの移行には、XOR暗号化に使用していた暗号化キー（server.iniのencrypt_key）が必要。
	  キーが異なると復号結果が誤ったままハッシュ化され、ログインできなくなる
	  （その場合はバックアップから戻して、正しいキーで再実行する）。
	- サーバーを停止した状態で実行すること（setup/migrate_data.py）。
	"""

	# 旧定義のtbl_authの権限キー列
	LEGACY_AUTH_KEY = "auth_id"

	# 旧setup.sqlのmst_authの列名 → 現在の定義の列名
	LEGACY_AUTH_MASTER_COLUMNS = {"auth_id": "id", "auth_name": "name"}

	# tbl_account.passwordに必要な長さ。これより短い文字列型の場合は拡張する
	# （パスワードハッシュ（bcrypt-sha256）は75文字。形式の変更に備えて余裕を持たせる）
	PASSWORD_COLUMN_MIN_LENGTH = 255

	def __init__(self, ctrl_db, ctrl_define, encrypt_key, log=print):
		"""
		コンストラクタ

		Parameters
		----------
		ctrl_db : functions.common.control_db.ControlDB
			DB制御クラス
		ctrl_define : functions.common.control_define.ControlDefine
			外部定義制御クラス
		encrypt_key : string
			XOR暗号化に使用していた暗号化キー
		log : callable, default print
			進捗の出力先
		"""
		self.ctrl_db = ctrl_db
		self.ctrl_define = ctrl_define
		self.encrypt_key = encrypt_key
		self.log = log
		# バックアップ名の接尾辞（1回の実行で共通）
		self.suffix = datetime.now().strftime("%Y%m%d%H%M%S")
		# バックアップ済みのテーブル・ファイル（1回の実行で同じ対象を二重に取らない）
		self.backed_up = set()

	def exec(self):
		"""
		移行処理

		Returns
		-------
		list of string
			パスワードを移行できなかったアカウントのuser_id
		"""
		ctrl = self.ctrl_db["db_control"]
		# 権限キーの変換はmst_authを参照するため、mst_authを先に現在の定義の列名にする
		self.migrate_auth_master(ctrl)
		# 権限キーの変換は、列の追加より先に行う（先に列を追加すると、旧定義のtbl_authへ
		# function列だけが追加され、変換が必要かを判定できなくなるため）
		self.migrate_auth(ctrl)
		self.migrate_account_token(ctrl)
		for db_id in self.ctrl_db.keys():
			self.migrate_schema(self.ctrl_db[db_id])
		# パスワードハッシュを格納できるよう、パスワードの移行より先に列を広げる
		self.widen_password_column(ctrl)
		return self.migrate_account(ctrl)

	def migrate_schema(self, ctrl):
		"""
		列の追加処理

		定義にあり、DBに存在しないテーブル・列を追加する。既存の列の変更・削除は行わない。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス
		"""
		if isinstance(ctrl, ControlTinyDB):
			return
		for tbl_id, def_tbl in ctrl.tables.items():
			columns = self.get_columns(ctrl, tbl_id)
			if len(columns) == 0:
				ctrl.create_table(tbl_id)
				self.log("テーブルを作成しました: {0}".format(tbl_id))
				continue
			for key in def_tbl.get("column", []):
				if "join" in key["role"] or key.get("key", "") == "":
					continue
				if key["key"].lower() in columns:
					continue
				if ctrl.is_autoincrement(key):
					# 採番列は主キーのため既存のテーブルへ追加できない
					self.log("採番列は追加できないため、手動で移行してください: {0}.{1}".format(
						tbl_id, key["key"]))
					continue
				ctrl.exec_sql("alter table {0} add column {1} {2}".format(
					tbl_id, key["key"], ctrl.get_column_type(key)))
				self.log("列を追加しました: {0}.{1}".format(tbl_id, key["key"]))

	def migrate_auth_master(self, ctrl):
		"""
		mst_authの列名の変更処理

		旧来のセットアップ（setup/setup.sql）はmst_authを(auth_id, auth_name, function, operation)で
		作成していたが、現在の定義は(id, name, function, operation)のため、列名を変更する。
		列名の変更は値（ID・名称）を保ち、ほかのテーブルからの外部キーの参照
		（mst_menu_auth.auth_id → mst_auth.auth_id）もDBが新しい列名へ付け替える。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（db_control）

		Notes
		-----
		ALTER TABLE ... RENAME COLUMNを使用する（SQLite 3.25以降、MySQL 8.0以降、MariaDB 10.5.2以降）。
		"""
		if isinstance(ctrl, ControlTinyDB) or not "mst_auth" in ctrl.tables.keys():
			return
		columns = self.get_columns(ctrl, "mst_auth")
		renames = [(old, new) for old, new in self.LEGACY_AUTH_MASTER_COLUMNS.items()
				   if old in columns and not new in columns]
		if len(renames) == 0:
			return
		self.backup(ctrl, "mst_auth")
		for old, new in renames:
			ctrl.exec_sql("alter table mst_auth rename column {0} to {1}".format(old, new))
			self.log("列名を変更しました: mst_auth.{0} → {1}".format(old, new))

	def migrate_auth(self, ctrl):
		"""
		tbl_authの権限キーの変換処理

		旧定義のtbl_authは権限定義のID（auth_id = mst_auth.id）を保持していたが、
		現在の定義は機能名（function = mst_auth.function）で結合するため、
		mst_authを引いて変換する。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（db_control）
		"""
		if not "tbl_auth" in ctrl.tables.keys():
			return
		functions = self.get_auth_functions(ctrl)
		if isinstance(ctrl, ControlTinyDB):
			table = ctrl.db.table("tbl_auth")
			docs = [doc for doc in table.all() if self.LEGACY_AUTH_KEY in doc]
			if len(docs) == 0:
				return
			self.backup(ctrl, "tbl_auth")
			for doc in docs:
				auth_id = doc[self.LEGACY_AUTH_KEY]
				table.update({"function": functions.get(auth_id, auth_id)}, doc_ids=[doc.doc_id])
				table.update(delete(self.LEGACY_AUTH_KEY), doc_ids=[doc.doc_id])
			self.log("tbl_authの権限キーを変換しました: {0}件".format(len(docs)))
			return

		columns = self.get_columns(ctrl, "tbl_auth")
		if not self.LEGACY_AUTH_KEY in columns:
			return
		# 主キー（id, auth_id → id, function）が変わるため、テーブルを作り直す。
		# MySQLのDDLは暗黙にコミットされロールバックできないため、元のテーブルを消す前に、
		# 新しい定義の作業用テーブルを作成・投入し、成功してから入れ替える
		# （作業用テーブルの作成・投入に失敗しても、元のテーブルはそのまま残る）
		rows = ctrl.exec_sql("select * from tbl_auth")
		self.backup(ctrl, "tbl_auth")
		insert_data = []
		for row in rows:
			auth_id = row[self.LEGACY_AUTH_KEY]
			insert_data.append({
				"id": row["id"],
				"function": functions.get(auth_id, auth_id),
				"auth_value": row.get("auth_value")
			})
		work_id = "tbl_auth_mig_{0}".format(self.suffix)
		ctrl.tables[work_id] = ctrl.tables["tbl_auth"]
		try:
			ctrl.begin()
			try:
				ctrl.create_table(work_id)
				if len(insert_data) > 0:
					ctrl.insert(work_id, insert_data, True)
				ctrl.commit()
			except Exception:
				ctrl.rollback()
				# MySQLでは作成したテーブルがロールバックで消えないため削除する
				ctrl.exec_sql("drop table if exists {0}".format(work_id))
				raise
		finally:
			del ctrl.tables[work_id]
		self.swap_table(ctrl, "tbl_auth", work_id)
		self.log("tbl_authの権限キーを変換しました: {0}件".format(len(rows)))

	def migrate_account_token(self, ctrl):
		"""
		tbl_account_tokenの移行処理

		旧来のセットアップ（setup/setup.sql）はtbl_account_tokenを
		(id INTEGER, token VARCHAR(50), expires_at DATE, revoked)・主キー(id, token)で作成していた。
		列名は現在の定義と同じだが、JWTはVARCHAR(50)に収まらず、DATEでは無操作タイムアウトの
		時刻を保持できない。主キーに(id, token)を含むため、ログイン時のUPSERT（refresh_token）で
		アカウントの前のトークンを置き換えることもできない。
		既存のトークンは上記のとおり有効なセッションとして使えないため、
		現在の定義のテーブルへ作り直し、既存のログインセッションは破棄する
		（利用者は再ログインが必要。破棄前の行はバックアップに残る）。

		TinyDBは型の制約がないため作り直さないが、アカウントID（id）を数値で保存した行は、
		延長・無効化（JWTのsubの文字列で照合する）の対象にならないため、文字列にそろえる。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（db_control）
		"""
		if not "tbl_account_token" in ctrl.tables.keys():
			return
		if isinstance(ctrl, ControlTinyDB):
			table = ctrl.db.table("tbl_account_token")
			docs = [doc for doc in table.all() if "id" in doc and not isinstance(doc["id"], str)]
			if len(docs) == 0:
				return
			self.backup(ctrl, "tbl_account_token")
			for doc in docs:
				table.update({"id": str(doc["id"])}, doc_ids=[doc.doc_id])
			self.log("tbl_account_tokenのアカウントIDを文字列にそろえました: {0}件".format(len(docs)))
			return

		columns = self.get_column_info(ctrl, "tbl_account_token")
		if len(columns) == 0:
			return
		keys = set(k["key"].lower() for k in ctrl.tables["tbl_account_token"].get("column", [])
				   if "key" in k["role"] and not "join" in k["role"])
		primary_keys = set(name for name, info in columns.items() if info["pk"])
		if (primary_keys == keys
				and columns.get("token", {}).get("type", "").startswith("text")
				and columns.get("expires_at", {}).get("type", "") != "date"):
			return
		self.backup(ctrl, "tbl_account_token")
		work_id = "tbl_account_token_mig_{0}".format(self.suffix)
		ctrl.tables[work_id] = ctrl.tables["tbl_account_token"]
		try:
			try:
				ctrl.create_table(work_id)
			except Exception:
				ctrl.exec_sql("drop table if exists {0}".format(work_id))
				raise
		finally:
			del ctrl.tables[work_id]
		self.swap_table(ctrl, "tbl_account_token", work_id)
		self.log("tbl_account_tokenを作り直しました（既存のログインセッションは破棄しました。再ログインが必要です）")

	def swap_table(self, ctrl, tbl_id, work_id):
		"""
		テーブルの入れ替え処理

		作業用テーブル（work_id）をtbl_idへ改名し、元のテーブルを削除する。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（SQLite・MySQL）
		tbl_id : string
			入れ替えるテーブルID
		work_id : string
			作業用テーブルID
		"""
		old_id = "{0}_old_{1}".format(tbl_id, self.suffix)
		if isinstance(ctrl, ControlSqlite):
			# SQLiteはDDLもトランザクションで扱えるため、改名・削除をまとめてコミットする
			ctrl.begin()
			try:
				ctrl.exec_sql("alter table {0} rename to {1}".format(tbl_id, old_id))
				ctrl.exec_sql("alter table {0} rename to {1}".format(work_id, tbl_id))
				ctrl.exec_sql("drop table {0}".format(old_id))
				ctrl.commit()
			except Exception:
				ctrl.rollback()
				raise
			return
		# MySQLのRENAME TABLEは複数の改名を不可分に行う（途中でtbl_idが存在しない状態にならない）
		ctrl.exec_sql("rename table {0} to {1}, {2} to {0}".format(tbl_id, old_id, work_id))
		ctrl.exec_sql("drop table {0}".format(old_id))

	def widen_password_column(self, ctrl):
		"""
		tbl_account.passwordの列の拡張処理（MySQL）

		旧来のセットアップ（setup/setup.sql）はpasswordをVARCHAR(30)で作成していたが、
		パスワードハッシュ（bcrypt-sha256）は75文字のため格納できない（SQLモードにより、
		エラーになるか切り詰められて照合できなくなる）。パスワードを移行する前に、
		短い文字列型の場合はテーブル定義の型へ変更する。SQLiteは長さを制約しないため不要。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（db_control）
		"""
		if isinstance(ctrl, (ControlTinyDB, ControlSqlite)) or not "tbl_account" in ctrl.tables.keys():
			return
		key = next((k for k in ctrl.tables["tbl_account"].get("column", [])
					if k.get("key") == "password"), None)
		if key is None:
			return
		rows = ctrl.exec_sql(
			"select data_type as data_type, character_maximum_length as length, is_nullable as is_nullable"
			" from information_schema.columns"
			" where table_schema = database() and table_name = %s and column_name = %s",
			("tbl_account", "password"))
		if len(rows) == 0:
			return
		column = rows[0]
		if (str(column["data_type"]).lower() not in ("char", "varchar")
				or column["length"] is None or int(column["length"]) >= self.PASSWORD_COLUMN_MIN_LENGTH):
			return
		ctrl.exec_sql("alter table tbl_account modify column password {0}{1}".format(
			ctrl.get_column_type(key),
			" not null" if str(column["is_nullable"]).upper() == "NO" else ""))
		self.log("列の型を変更しました: tbl_account.password（{0}({1}) → {2}）".format(
			column["data_type"], column["length"], ctrl.get_column_type(key)))

	def get_auth_functions(self, ctrl):
		"""
		権限定義のIDと機能名（mst_auth.function）の対応の取得処理

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（db_control）

		Returns
		-------
		dict
			{権限定義のID: 機能名}。mst_authに存在しないIDは権限定義（auth.xml）のref_name
		"""
		result = {}
		for auth in self.ctrl_define["auth"]["def"].values():
			if auth.get("ref_name"):
				result[auth["id"]] = auth["ref_name"]
		if isinstance(ctrl, ControlTinyDB) or len(self.get_columns(ctrl, "mst_auth")) > 0:
			for record in ctrl.select("mst_auth"):
				if record.get("function"):
					result[record["id"]] = record["function"]
		return result

	def migrate_account(self, ctrl):
		"""
		tbl_accountの追加列の補完・パスワードの移行処理

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（db_control）

		Returns
		-------
		list of string
			パスワードを移行できなかったアカウントのuser_id
		"""
		if not "tbl_account" in ctrl.tables.keys():
			return []
		now = datetime.now().isoformat()
		failed = []
		count_password = 0
		count_backfill = 0
		ctrl.begin()
		try:
			for row in ctrl.select("tbl_account"):
				password = row.get("password")
				# 未設定（NULL）の列だけを補完する。作成者・更新者は不明なため本人とする
				# emailは未設定（空文字はSQL系のDBではNULLとして保存される）のままとする
				defaults = {
					# 既存のアカウントはパスワードを設定済みとして扱う
					"is_password_set": not UtilCheck.is_empty(password),
					"login_failed_count": 0,
					"created_at": now,
					"created_by": row["id"],
					"updated_at": now,
					"updated_by": row["id"]
				}
				update = {k: v for k, v in defaults.items() if row.get(k) is None}
				if len(update) > 0:
					count_backfill += 1
				if not UtilCheck.is_empty(password) and not UtilEncrypt.is_hashed_password(password):
					try:
						plain = UtilEncrypt.decrypt_xor(password, self.encrypt_key)
					except (ValueError, UnicodeDecodeError):
						failed.append(row.get("user_id"))
						plain = None
					if not UtilCheck.is_empty(plain):
						update["password"] = UtilEncrypt.hash_password(plain)
						count_password += 1
				if len(update) == 0:
					continue
				self.backup(ctrl, "tbl_account")
				ctrl.update("tbl_account", update, {"id": row["id"]})
			ctrl.commit()
		except Exception:
			ctrl.rollback()
			raise
		self.log("tbl_accountの列を補完しました: {0}件".format(count_backfill))
		self.log("パスワードをbcryptへ移行しました: {0}件".format(count_password))
		for user_id in failed:
			self.log("パスワードを復号できないため移行できませんでした: {0}".format(user_id))
		return failed

	def get_columns(self, ctrl, tbl_id):
		"""
		DBに存在する列名の取得処理

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（SQLite・MySQL）
		tbl_id : string
			テーブルID

		Returns
		-------
		set of string
			列名（小文字）。テーブルが存在しない場合は空
		"""
		if isinstance(ctrl, ControlSqlite):
			rows = ctrl.exec_sql("pragma table_info({0})".format(tbl_id))
		else:
			rows = ctrl.exec_sql(
				"select column_name as name from information_schema.columns"
				" where table_schema = database() and table_name = %s", (tbl_id,))
		return set(str(row["name"]).lower() for row in rows)

	def get_column_info(self, ctrl, tbl_id):
		"""
		DBに存在する列の型・主キーの取得処理

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス（SQLite・MySQL）
		tbl_id : string
			テーブルID

		Returns
		-------
		dict
			{列名（小文字）: {"type": 列型（小文字。例: "varchar(50)"）, "pk": 主キーか}}。
			テーブルが存在しない場合は空
		"""
		if isinstance(ctrl, ControlSqlite):
			rows = ctrl.exec_sql("pragma table_info({0})".format(tbl_id))
			return {str(row["name"]).lower(): {"type": str(row["type"]).lower(), "pk": row["pk"] > 0}
					for row in rows}
		rows = ctrl.exec_sql(
			"select column_name as name, column_type as type, column_key as column_key"
			" from information_schema.columns"
			" where table_schema = database() and table_name = %s", (tbl_id,))
		return {str(row["name"]).lower(): {"type": str(row["type"]).lower(), "pk": row["column_key"] == "PRI"}
				for row in rows}

	def backup(self, ctrl, tbl_id):
		"""
		バックアップ処理

		変更する前に1回だけ取る（SQLite・MySQL: テーブルの複製、TinyDB: DBファイルの複製）。

		Parameters
		----------
		ctrl : functions.data.control_base.ControlBase
			DB制御クラス
		tbl_id : string
			変更するテーブルID
		"""
		if isinstance(ctrl, ControlTinyDB):
			if ctrl.db_path is None or ctrl.db_path in self.backed_up or not os.path.exists(ctrl.db_path):
				return
			path = "{0}.bak_{1}".format(ctrl.db_path, self.suffix)
			shutil.copy2(ctrl.db_path, path)
			self.backed_up.add(ctrl.db_path)
			self.log("バックアップしました: {0}".format(path))
			return
		if (ctrl, tbl_id) in self.backed_up:
			return
		bak_id = "{0}_bak_{1}".format(tbl_id, self.suffix)
		ctrl.exec_sql("create table {0} as select * from {1}".format(bak_id, tbl_id))
		self.backed_up.add((ctrl, tbl_id))
		self.log("バックアップしました: {0}".format(bak_id))
