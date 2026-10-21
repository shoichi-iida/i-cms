# -*- coding: utf-8 -*-

import pymysql.cursors
from functions.data.control_base import ControlBase

class ControlMysql(ControlBase):
	"""
	MYSQL制御クラス

	値はすべてプレースホルダ（"%s"）でバインドする。ControlBaseの
	get_*_sqlが返す(sql, params)をそのままexec_sqlへ渡す。
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
		self.placeholder = "%s"
		# トランザクション中か（begin の二重発行を避けるため。pymysql の begin は
		# 実行中のトランザクションを暗黙にコミットしてしまう）
		self.in_transaction = False
		# 書き込みロック（GET_LOCK）を持っているか（issue #207）
		self.write_lock_held = False
		# 書き込みロックの名前。同じDBへの書き込みだけを直列化する（GET_LOCKの名前は64文字まで）
		self.write_lock_name = "write:{0}".format(db_name)[:64]
		self.autoincrement_keyword = "auto_increment"
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

	# 書き込みロック（GET_LOCK）の解放を待つ秒数（SQLiteのBUSY_TIMEOUT_SECONDSと同じ）
	WRITE_LOCK_TIMEOUT_SECONDS = 30

	def begin(self, serialize=None):
		"""
		トランザクション開始処理

		serialize（Noneなら serialize_writes）がTrueなら、トランザクションの前に
		名前付きの書き込みロック（GET_LOCK）を取る。同じロックを取る
		ほかの接続（更新系のリクエスト）はコミット・ロールバックまで待たされるため、
		トランザクション内の判定から書き込みまでが直列化される。InnoDBの行ロック
		（select の for_update）と違い、まだ存在しない行（重複チェックの後のINSERT）も守れる。
		既にトランザクション中なら BEGIN を発行しない（pymysql の begin は実行中の
		トランザクションを暗黙にコミットするため）。
		直列化していないトランザクションの中で直列化を求められた場合は、ロックを取らずに
		例外にする（ControlSqlite.begin と同じ）。REPEATABLE READ ではトランザクションの
		最初の読み取り時点のスナップショットを読み続けるため、途中でロックを取っても、
		ロックを取るまでにほかの接続がコミットした変更を見落とし、判定と書き込みが競合する
		（直列化するリクエストは最初に begin する。ApiHandler.call_api）
		"""
		if serialize is None:
			serialize = self.serialize_writes
		if serialize and self.in_transaction and not self.write_lock_held:
			raise Exception("直列化していないトランザクションの中では、書き込みを直列化できません")
		if serialize and not self.write_lock_held:
			rows = self.exec_sql("select get_lock(%s, %s) as acquired",
								 (self.write_lock_name, self.WRITE_LOCK_TIMEOUT_SECONDS))
			if not rows or rows[0].get("acquired") != 1:
				raise Exception("書き込みロックを取得できませんでした（{0}）".format(
					self.write_lock_name))
			self.write_lock_held = True
		if not self.in_transaction:
			try:
				self.db.begin()
			except Exception:
				# 書き込みロックを取ったまま BEGIN に失敗した場合は、ロックを解放する
				# （接続を閉じるまでほかのプロセスの書き込みを止めないため）
				self.release_write_lock()
				raise
			self.in_transaction = True

	def commit(self):
		"""
		トランザクションコミット処理（書き込みロックを持っていれば解放する）
		"""
		try:
			self.db.commit()
		finally:
			self.in_transaction = False
			self.release_write_lock()

	def rollback(self):
		"""
		トランザクションロールバック処理（書き込みロックを持っていれば解放する）
		"""
		try:
			self.db.rollback()
		finally:
			self.in_transaction = False
			self.release_write_lock()

	def release_write_lock(self):
		"""書き込みロック（GET_LOCK）を持っていれば解放する"""
		if self.write_lock_held:
			self.write_lock_held = False
			self.exec_sql("select release_lock(%s) as released", (self.write_lock_name,))

	def get_cursor(self):
		return self.db.cursor()

	def exec_sql(self, sql, param=()):
		if sql is None:
			return []
		with self.db.cursor() as cursor:
			# バインド値がない場合はargsを渡さない。pymysqlはargsが指定された
			# ときだけSQL文へ%書式を適用するため、値のないSQL（DDL等）に含まれる
			# リテラルの%を壊さずに済む。値がある場合のSQLはget_*_sqlが生成した
			# もので、プレースホルダ以外に%を含まない。
			if param:
				cursor.execute(sql, param)
			else:
				cursor.execute(sql)
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
			Trueなら FOR UPDATE で取得した行をトランザクションの終わりまでロックする
		"""
		sql, params = super().get_select_sql(
			tbl_id, dict_select, lst_exclude, fixed_where, sort_order, for_update)
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

		id列を持つテーブルでは、SQLite（returning句）と同じく挿入した行のidを
		[{"id": ...}]の形でレコードごとに返す。
		MySQLにはreturning句がないため、idを指定した場合はその値を、指定しない場合
		（AUTO_INCREMENTで採番）と、AUTO_INCREMENTの列へ0を指定した場合は
		cursor.lastrowidを返す。

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_insert : list of dict
			挿入情報配列。
			[{ "key1" : "value1", "key2" : "value2"}]のようにキー項目と値を設定する。
		"""
		id_column = self.get_id_column(tbl_id)
		result = []
		lst_sql = super().get_insert_sql(tbl_id, lst_insert, is_upsert) or []
		# get_insert_sqlはレコードごとに1件ずつ、同じ順序で(sql, params)を返す
		for record, (sql, params) in zip(lst_insert, lst_sql):
			with self.db.cursor() as cursor:
				cursor.execute(sql, params)
				if id_column is None:
					result.extend(cursor.fetchall())
					continue
				record_id = self.get_value(id_column, record)
				if self.is_autoincrement(id_column) and isinstance(record_id, str):
					# 採番列は整数のため、整数の文字列（"0"等）は数値として扱う
					try:
						record_id = int(record_id)
					except ValueError:
						pass
				# AUTO_INCREMENTの列へ0を指定した場合も、MySQLは（NO_AUTO_VALUE_ON_ZERO
				# モードでなければ）採番するため、cursor.lastrowidを返す。
				# NO_AUTO_VALUE_ON_ZEROモードで0がそのまま保存された場合は、lastrowidも0になる
				if (record_id is None or (self.is_autoincrement(id_column) and record_id == 0)) \
						and cursor.lastrowid is not None:
					record_id = cursor.lastrowid
				result.append({"id": record_id})
		return result

	def get_id_column(self, tbl_id):
		"""
		テーブル定義のid列を返す。id列を持たない場合はNoneを返す。

		SQLiteのget_insert_sqlがreturning句を付ける条件（join・like以外の"id"列）と揃える。
		"""
		for key in self.tables.get(tbl_id, {}).get("column", []):
			if "join" in key["role"] or "like" in key["role"]:
				continue
			if key.get("key") == "id":
				return key
		return None

	# 主キーに含める文字列列の型。MySQLはTEXT型の列を長さ指定なしで主キーにできないため、
	# 可変長文字列にする（utf8mb4で255文字×4バイト＝1020バイト。3列の複合キーでも
	# InnoDBのインデックスの上限（3072バイト）に収まる）
	KEY_TEXT_COLUMN_TYPE = "varchar(255)"

	def get_column_type(self, key):
		"""
		列定義に対応するDBの列型の取得処理

		主キーのtext列はKEY_TEXT_COLUMN_TYPEにする（MySQLはTEXT型の列を
		長さ指定なしで主キーにできないため）。それ以外はControlBaseと同じ。

		Parameters
		----------
		key : dict
			列定義（テーブル定義XMLのcolumn）

		Returns
		-------
		string
			列型
		"""
		if key["type"] == "text" and "key" in key["role"]:
			return self.KEY_TEXT_COLUMN_TYPE
		return super().get_column_type(key)

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
