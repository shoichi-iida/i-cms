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

	1. 定義に追加された列の追加（SQLite・MySQL。TinyDBは列の概念がないため不要）
	2. tbl_authの権限キーの変換（旧: auth_id = mst_auth.id → 新: function = mst_auth.function）
	3. tbl_accountの追加列の補完（is_password_set・login_failed_count・作成/更新日時等）
	4. パスワードの移行（XOR暗号 → bcrypt-sha256）

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
		# 権限キーの変換は、列の追加より先に行う（先に列を追加すると、旧定義のtbl_authへ
		# function列だけが追加され、変換が必要かを判定できなくなるため）
		self.migrate_auth(ctrl)
		for db_id in self.ctrl_db.keys():
			self.migrate_schema(self.ctrl_db[db_id])
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
		# 主キー（id, auth_id → id, function）が変わるため、テーブルを作り直す
		rows = ctrl.exec_sql("select * from tbl_auth")
		self.backup(ctrl, "tbl_auth")
		ctrl.begin()
		try:
			ctrl.drop_table("tbl_auth")
			ctrl.create_table("tbl_auth")
			insert_data = []
			for row in rows:
				auth_id = row[self.LEGACY_AUTH_KEY]
				insert_data.append({
					"id": row["id"],
					"function": functions.get(auth_id, auth_id),
					"auth_value": row.get("auth_value")
				})
			if len(insert_data) > 0:
				ctrl.insert("tbl_auth", insert_data, True)
			ctrl.commit()
		except Exception:
			ctrl.rollback()
			raise
		self.log("tbl_authの権限キーを変換しました: {0}件".format(len(rows)))

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
