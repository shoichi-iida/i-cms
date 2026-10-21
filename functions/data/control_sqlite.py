# -*- coding: utf-8 -*-

import os
import sqlite3
from functions.data.control_base import ControlBase

class ControlSqlite(ControlBase):
	"""
	SQLite制御クラス

	値はすべてプレースホルダ（"?"）でバインドする。ControlBaseの
	get_*_sqlが返す(sql, params)をそのままexec_sqlへ渡す。
	"""
	# ほかの接続が書き込みロックを持っているときに待つ秒数（sqlite3.connectのtimeout）。
	# 更新系のリクエストは BEGIN IMMEDIATE で直列化するため、既定の5秒より
	# 長めに待つ。待ちきれない場合は sqlite3.OperationalError（database is locked）になる
	BUSY_TIMEOUT_SECONDS = 30

	def __init__(self, db_path=":memory:", timeout=BUSY_TIMEOUT_SECONDS):
		"""
		コンストラクタ

		Parameters
		----------
		db_path : string, default ":memory:"
			DB保存パス。未指定の場合はインメモリで動作。
		timeout : float, default BUSY_TIMEOUT_SECONDS
			ほかの接続のロックの解放を待つ秒数
		"""
		super().__init__()
		self.make_db_dir(db_path)
		self.placeholder = "?"
		self.autoincrement_keyword = "autoincrement"
		self.db = sqlite3.connect(db_path, timeout=timeout)
		self.db.row_factory = self.dict_factory
		self.db.isolation_level = None
		# 今のトランザクションを BEGIN IMMEDIATE（直列化）で始めたか（issue #207）
		self.serialized_transaction = False

	def dict_factory(self, cursor, row):
		d = {}
		for idx, col in enumerate(cursor.description):
			d[col[0]] = row[idx]
		return d

	def __del__(self):
		"""
		デストラクタ
		"""
		super().__del__()
		self.db.close()

	def begin(self, serialize=None):
		"""
		トランザクション開始処理

		serialize（Noneなら serialize_writes）がTrueなら BEGIN IMMEDIATE で開始し、
		開始の時点で書き込みロック（RESERVED）を取る。ほかの接続の
		BEGIN IMMEDIATE・書き込みはコミットまで待たされるため、トランザクション内の
		判定（重複・存在・楽観ロックなど）から書き込みまでが直列化される。
		ほかの接続の読み取りは妨げない。既にトランザクション中なら何もしない
		（素の BEGIN で始めたトランザクションを途中から直列化することはできないため、
		直列化するリクエストは最初に begin する。ApiHandler.call_api）
		"""
		if serialize is None:
			serialize = self.serialize_writes
		if self.db.in_transaction:
			# 素の BEGIN で始めたトランザクションの中で直列化を求められた場合は、黙って
			# 直列化しないまま進めず、例外にする（途中から書き込みロックへ切り替えられず、
			# 直列化したつもりの処理が競合を残すため）
			if serialize and not self.serialized_transaction:
				raise sqlite3.OperationalError(
					"直列化していないトランザクションの中では、書き込みを直列化できません")
			return
		self.db.execute("BEGIN IMMEDIATE" if serialize else "BEGIN")
		self.serialized_transaction = bool(serialize)

	def commit(self):
		"""
		トランザクションコミット処理
		"""
		if self.db.in_transaction:
			self.db.commit()

	def rollback(self):
		"""
		トランザクションロールバック処理
		"""
		if self.db.in_transaction:
			self.db.rollback()

	def get_cursor(self):
		return self.db.cursor()

	def exec_sql(self, sql, param=()):
		if sql is None:
			return []
		cursor = self.get_cursor()
		cursor.execute(sql, param)
		return cursor.fetchall()

	def create_table(self, tbl_id):
		"""
		テーブル作成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		"""
		return self.exec_sql(super().get_create_table_sql(tbl_id))

	def drop_table(self, tbl_id):
		"""
		テーブル削除処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		"""
		return self.exec_sql(super().get_drop_table_sql(tbl_id))

	def select(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[], sort_order=[],
			   for_update=False):
		"""
		レコード取得処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_select : dict, default {}
			取得対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		lst_exclude : list of string, default []
			取得除外対象配列。
		fixed_where : list of string default []
			固定WHERE句配列。
		for_update : bool, default False
			SQLiteは FOR UPDATE を持たないため使わない（I/F互換のため受け取る）。
			行のロックの代わりに、直列化したトランザクション（begin(serialize=True)）で
			書き込みを直列化する（issue #207）
		"""
		sql, params = super().get_select_sql(tbl_id, dict_select, lst_exclude, fixed_where, sort_order)
		return self.exec_sql(sql, params)

	def distinct(self, tbl_id, lst_select=[], dict_select={}):
		"""
		重複排除取得SQL生成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_select : list of string, default []
			取得対象列配列。
		dict_select : dict, default {}
			取得対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		"""
		sql, params = super().get_distinct_sql(tbl_id, lst_select, dict_select)
		return self.exec_sql(sql, params)

	def insert(self, tbl_id, lst_insert, is_upsert=False):
		"""
		レコード挿入処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_insert : list of dict
			挿入情報配列。
			[{ "key1" : "value1", "key2" : "value2"}]のようにキー項目と値を設定する。
		"""
		result = []
		for sql, params in self.get_insert_sql(tbl_id, lst_insert, is_upsert) or []:
			result.extend(self.exec_sql(sql, params))
		return result

	def get_insert_sql(self, tbl_id, lst_insert, is_upsert=False):
		"""
		レコード挿入SQL生成処理

		SQLiteのUPSERT構文（on conflict do update）と、採番したidを取得する
		returning句を使うためControlBaseの実装を上書きする。

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_insert : list of dict
			挿入情報配列。
			[{ "key1" : "value1", "key2" : "value2"}]のようにキー項目と値を設定する。

		Returns
		-------
		list of (sql, params)
			レコードごとのSQL文とバインド値の組の配列。
			テーブル未定義の場合はNoneを返す。
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		result = []
		for record in lst_insert:
			col_text = []
			set_text = []
			key_text = []
			update_text = []
			set_params = []
			update_params = []
			is_id = False
			for key in def_tbl.get("column", []):
				if "join" in key["role"] or "like" in key["role"] or key.get("key", "") == "":
					continue
				if key.get("key") == "id":
					is_id = True
				k = key['key']
				v = super().get_value(key, record)
				if "key" in key["role"]:
					key_text.append(f"`{k}`")
				else:
					update_text.append(f"`{k}` = {self.placeholder}")
					update_params.append(v)
				col_text.append(f"`{k}`")
				set_text.append(self.placeholder)
				set_params.append(v)
			sql = ["insert into {0} ({1}) values ({2})".format(
				tbl_id,
				", ".join(col_text),
				", ".join(set_text)
			)]
			params = list(set_params)
			if is_upsert:
				sql.append(" on conflict (")
				sql.append(", ".join(key_text))
				sql.append(") do update set ")
				sql.append(", ".join(update_text))
				params.extend(update_params)
			if is_id:
				sql.append(" returning `id` ")
			sql.append(";")
			result.append(("".join(sql), params))
		return result

	def update(self, tbl_id, dict_update, dict_where={}):
		"""
		レコード更新処理

		Parameters
		dict_update : dict
			更新情報辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		dict_where : dict, default {}
			更新対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		----------
		"""
		sql, params = super().get_update_sql(tbl_id, dict_update, dict_where)
		return self.exec_sql(sql, params)

	def increment(self, tbl_id, dict_increment, dict_where={}):
		"""
		指定した列の加算処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_increment : dict
			加算する列と加算値。{ "key1" : 1 }のように指定する。
		dict_where : dict, default {}
			更新対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		"""
		sql, params = super().get_increment_sql(tbl_id, dict_increment, dict_where)
		return self.exec_sql(sql, params)

	def delete(self, tbl_id, lst_delete=[]):
		"""
		レコード削除処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_delete : list of dict, default []
			削除対象条件配列。
			[{ "key1" : "value1", "key2" : "value2"}]のようにキー項目と値を設定する。
		"""
		result = []
		for sql, params in super().get_delete_sql(tbl_id, lst_delete) or []:
			result.extend(self.exec_sql(sql, params))
		return result
