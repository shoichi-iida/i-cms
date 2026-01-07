# -*- coding: utf-8 -*-

import pymysql.cursors
from functions.data.control_base import ControlBase

class ControlMysql(ControlBase):
	"""
	MYSQL制御クラス
	"""
	def __init__(self, db_host, db_port, db_user, db_password, db_name, db_charset):
		"""
		コンストラクタ

		Parameters
		----------
		db_host : string
			DB接続ホスト名
		db_user : string
			DB接続ユーザ名
		db_password : string
			DB接続パスワード
		db_name : string
			接続先DB名
		db_charset : string
			DB接続文字コード
		"""
		super().__init__()
		self.db = pymysql.connect(
			host=db_host,
			port=db_port,
			user=db_user,
			password=db_password,
			db=db_name,
			charset=db_charset,
			cursorclass=pymysql.cursors.DictCursor)

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
		self.db.begin()

	def commit(self):
		"""
		トランザクションコミット処理
		"""
		self.db.commit()

	def rollback(self):
		"""
		トランザクションロールバック処理
		"""
		self.db.rollback()

	def get_cursor(self):
		return self.db.cursor()

	def exec_sql(self, sql, param=()):
		with self.db.cursor() as cursor:
			cursor.execute(sql.replace('%', '%%'), param)
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

	def select(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[]):
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
		return self.exec_sql(super().get_select_sql(tbl_id, dict_select, lst_exclude, fixed_where))

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
		for sql in super().get_insert_sql(tbl_id, lst_insert, is_upsert):
			result.extend(self.exec_sql(sql))
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
		return self.db.escape(val)
