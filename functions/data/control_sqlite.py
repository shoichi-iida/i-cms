# -*- coding: utf-8 -*-

import os
import sqlite3
from functions.data.control_base import ControlBase

class ControlSqlite(ControlBase):
	"""
	SQLite制御クラス
	"""
	def __init__(self, db_path=":memory:"):
		"""
		コンストラクタ

		Parameters
		----------
		db_path : string, default ":memory:"
			DB保存パス。未指定の場合はインメモリで動作。
		"""
		super().__init__()
		self.make_db_dir(db_path)
		self.db = sqlite3.connect(db_path)
		self.db.row_factory = self.dict_factory
		self.db.isolation_level = None

	def make_db_dir(self, db_path):
		"""
		DB保存ディレクトリ作成処理

		DBファイルの格納先ディレクトリが存在しない場合に作成する。
		インメモリ指定の場合は何もしない。

		Parameters
		----------
		db_path : string
			DB保存パス
		"""
		if not db_path or db_path == ":memory:":
			return
		db_dir = os.path.dirname(os.path.abspath(db_path))
		if os.path.isdir(db_dir):
			return
		os.makedirs(db_dir, exist_ok=True)

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

	def begin(self):
		"""
		トランザクション開始処理
		"""
		if not self.db.in_transaction:
			self.db.execute("BEGIN")

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

	def select(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[], sort_order=[]):
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
			固定WHERE区配列。
		"""
		return self.exec_sql(super().get_select_sql(tbl_id, dict_select, lst_exclude, fixed_where, sort_order))

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
		return self.exec_sql(super().get_distinct_sql(tbl_id, lst_select, dict_select))

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
		for sql in self.get_insert_sql(tbl_id, lst_insert, is_upsert):
			result.extend(self.exec_sql(sql))
		return result

	def get_insert_sql(self, tbl_id, lst_insert, is_upsert=False):
		"""
		レコード挿入SQL生成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_insert : list of dict
			挿入情報配列。
			[{ "key1" : "value1", "key2" : "value2"}]のようにキー項目と値を設定する。
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
			is_id = False
			for key in def_tbl.get("column", []):
				if "join" in key["role"] or "like" in key["role"] or key.get("key", "") == "":
					continue
				if key.get("key") == "id":
					is_id = True
				k = key['key']
				v = super().set_value_text(key, record)
				if "key" in key["role"]:
					key_text.append(f"`{k}`")
				else:
					update_text.append(f"`{k}` = {v}")
				col_text.append(f"`{k}`")
				set_text.append(v)
			sql = ["insert into {0} ({1}) values ({2})".format(
				tbl_id,
				", ".join(col_text),
				", ".join(set_text)
			)]
			if is_upsert:
				sql.append(" on conflict (")
				sql.append(", ".join(key_text))
				sql.append(") do update set ")
				sql.append(", ".join(update_text))
			if is_id:
				sql.append(" returning `id` ")
			sql.append(";")
			result.append("".join(sql))
		return result
	
	def update(self, tbl_id, dict_update, dict_where={}):
		"""
		レコード更新処理

		Parameters
		dict_update : dict
			更新情報辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		dict_where : dict, default []
			更新対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		----------
		"""
		return self.exec_sql(super().get_update_sql(tbl_id, dict_update, dict_where))

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
		return self.exec_sql(super().get_delete_sql(tbl_id, lst_delete))

	def escape(self, col_type, val):
		if not col_type in self.text_type:
			return "{0}".format(val)
		return "'{0}'".format(val)
