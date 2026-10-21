# -*- coding: utf-8 -*-
import os
from collections import OrderedDict
from abc import ABCMeta, abstractmethod

class ControlBase(metaclass=ABCMeta):
	"""
	DB制御基底クラス

	SQL生成メソッド（get_*_sql）は値をSQL文へ埋め込まず、プレースホルダと
	バインド値の組（sql, params）を返す。WHERE句・INSERT・UPDATEのすべての
	値がバインド対象となるため、値の内容がSQLの構文として解釈されることはない。

	プレースホルダの記法はDB製品ごとに異なるため、placeholder属性で切り替える
	（SQLite: "?"、MySQL: "%s"）。同様に、採番列のキーワードは
	autoincrement_keyword属性で切り替える（SQLite: "autoincrement"、
	MySQL: "auto_increment"）。

	テーブル名・列名・結合条件・ソート順はテーブル定義XML（define/tables）から
	組み立てるものでリクエスト値を含まないため、従来どおりSQL文へ埋め込む。
	"""
	def __init__(self):
		"""
		コンストラクタ
		"""
		self.tables = OrderedDict()
		# バインド変数のプレースホルダ（DB製品ごとにサブクラスで上書きする）
		self.placeholder = "?"
		# 採番列（テーブル定義XMLのautoincrement="true"）のキーワード
		# （DB製品ごとにサブクラスで上書きする）
		self.autoincrement_keyword = "auto_increment"
		# 書き込みを直列化するか。Trueの間は begin() の既定が、
		# 直列化されたトランザクション（SQLite: BEGIN IMMEDIATE、MySQL: 書き込みロック）になる。
		# ApiHandler が更新系（GET以外）のリクエストの間だけTrueにする
		self.serialize_writes = False

	def __del__(self):
		"""
		デストラクタ
		"""
		pass

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

	@abstractmethod
	def begin(self, serialize=None):
		"""
		トランザクション開始処理

		Parameters
		----------
		serialize : bool or None, default None
			Trueなら、ほかの接続の書き込みと直列化されたトランザクションを開始する。
			Noneなら serialize_writes 属性に従う
		"""
		pass

	@abstractmethod
	def commit(self):
		pass

	@abstractmethod
	def rollback(self):
		pass

	@abstractmethod
	def create_table(self, tbl_id):
		pass

	@abstractmethod
	def drop_table(self, tbl_id):
		pass

	@abstractmethod
	def select(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[]):
		pass

	@abstractmethod
	def distinct(self, tbl_id, lst_select=[], dict_select={}):
		pass

	@abstractmethod
	def insert(self, tbl_id, lst_insert, is_upsert=False):
		pass

	@abstractmethod
	def update(self, tbl_id, dict_update, dict_where={}):
		pass

	@abstractmethod
	def increment(self, tbl_id, dict_increment, dict_where={}):
		pass

	@abstractmethod
	def delete(self, tbl_id, lst_delete=[]):
		pass

	def get_create_table_sql(self, tbl_id):
		"""
		テーブル作成SQL生成処理

		キー列にautoincrement="true"を指定した場合、その列を
		「{列名} integer primary key {autoincrement_keyword}」とし、末尾の
		primary key句には含めない。SQLiteのAUTOINCREMENTは列制約としてしか
		書けないため（primary key(id)の形では指定できない）。

		AUTOINCREMENTの列は一度使った番号を二度と採番しない。指定がない整数の
		キー列はSQLiteではrowidの別名になり「その時点の最大値 + 1」で採番されるため、
		最大の番号の行を物理削除すると同じ番号がもう一度使われる。

		Parameters
		----------
		tbl_id : string
			テーブルID

		Raises
		------
		ValueError
			autoincrementを整数の単一キー列以外に指定した場合、または
			1テーブルの2列以上に指定した場合。
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		col_text = []
		key_text = []
		autoincrement_key = None
		for key in def_tbl.get("column", []):
			# 採番指定の妥当性は、DDLに現れない列（join列・列名なし）を読み飛ばす前に検証する
			# （読み飛ばしてから検証すると、誤った指定が黙って無視される）
			if self.is_autoincrement(key):
				if ("join" in key["role"] or key.get("key", "") == ""
						or not "key" in key["role"] or key.get("type") != "int"):
					raise ValueError("autoincrementは整数のキー列にのみ指定できます。（{0}.{1}）".format(
						tbl_id, key.get("key", "")))
				# 採番列は主キーそのものになるため、2列目の指定は主キーの重複になる
				if autoincrement_key is not None:
					raise ValueError("autoincrementは1テーブルに1列のみ指定できます。（{0}.{1}, {0}.{2}）".format(
						tbl_id, autoincrement_key, key["key"]))
				autoincrement_key = key["key"]
				col_text.append("{0} integer primary key {1}".format(
					key["key"], self.autoincrement_keyword))
				continue
			if "join" in key["role"] or key.get("key", "") == "":
				continue
			if "key" in key["role"]:
				key_text.append(key["key"])
			col_text.append("{0} {1}".format(key["key"], self.get_column_type(key)))
		# 採番列が主キーになるため、他のキー列と組み合わせた複合キーにはできない
		if autoincrement_key is not None and len(key_text) > 0:
			raise ValueError("autoincrementは単一キー列にのみ指定できます。（{0}.{1}）".format(
				tbl_id, autoincrement_key))
		if len(key_text) > 0:
			col_text.append("primary key({0})".format(", ".join(key_text)))
		return "create table if not exists {0} ({1});".format(
			tbl_id,
			", ".join(col_text)
		)

	def get_column_type(self, key):
		"""
		列定義に対応するDBの列型の取得処理

		Parameters
		----------
		key : dict
			列定義（テーブル定義XMLのcolumn）

		Returns
		-------
		string
			列型
		"""
		if key["type"] == "int":
			return "integer"
		elif key["type"] == "float":
			return "decimal"
		elif key["type"] == "bool":
			return "boolean"
		elif key["type"] == "datetime":
			return "datetime"
		elif key["type"] == "text":
			return "text"
		return "varchar(255)"

	def is_autoincrement(self, key):
		"""
		列定義が採番列（autoincrement="true"）かどうかの判定処理

		テーブル定義XMLの属性値は文字列のため、get_valueのbool正規化と
		同じく "true" / "1" を真とする。
		"""
		return str(key.get("autoincrement", "")).strip().lower() in ("true", "1")

	def get_drop_table_sql(self, tbl_id):
		"""
		テーブル削除SQL生成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		"""
		if not tbl_id in self.tables.keys():
			return None
		return "drop table if exists {0};".format(tbl_id)

	def get_select_sql(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[], sort_order=[],
					   for_update=False):
		"""
		レコード取得SQL生成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_select : list or dict, default {}
			取得対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
			配列で[{ "key1" : "value1" }, { "key2" : "value2" }]のように渡された場合はOR条件とする。
		lst_exclude : list of string, default []
			取得除外対象配列。
		fixed_where : list of string default []
			固定WHERE句配列。呼び出し側が組み立てるSQL断片であり、リクエスト値を
			含めてはならない（バインド対象外）。
		sort_order : list of dict default []
			ORDER句指定用配列。[{"key": "key1", "order": "desc"}]のように指定する。
		for_update : bool, default False
			Trueなら末尾に FOR UPDATE を付け、取得した行をトランザクションの終わりまで
			ロックする（issue #207。MySQL用。SQLiteは FOR UPDATE を持たないため
			ControlSqlite は指定しても付けず、BEGIN IMMEDIATE で書き込みを直列化する）

		Returns
		-------
		(sql, params) : (string, list)
			SQL文とバインド値の組。テーブル未定義の場合は(None, [])を返す。
		"""
		if not tbl_id in self.tables.keys():
			return None, []
		def_tbl = self.tables[tbl_id]
		tbl_name = def_tbl.get("name", tbl_id)
		select_text = []
		join_tbl = []
		lst_where_text = [[]]
		lst_where_params = [[]]
		sort_keys = [s["key"] for s in sort_order]
		sort_key_infos = {}
		sort_text = []
		result = []
		params = []
		# WHERE指定が配列の場合は複数の条件をOR句で結ぶ
		if isinstance(dict_select, list):
			lst_where_text = [[] for i in dict_select]
			lst_where_params = [[] for i in dict_select]
		for key in def_tbl.get("column", []):
			if key.get("key", "") == "":
				continue
			# SELECT句への追加
			if not key["key"] in lst_exclude:
				if key["role"] == "join":
					select_text.append("`{0}`.`{1}` as `{2}`".format(key["join"], key["get_key"], key["key"]))
					if not key["join"] in join_tbl:
						join_tbl.append(key["join"])
				else:
					select_text.append("`{0}`.`{1}`".format(tbl_name, key["key"]))
			# WHERE句の解釈
			if isinstance(dict_select, list):
				for idx, w in enumerate(dict_select):
					if key["role"] == "join":
						self.__append_where(lst_where_text[idx], lst_where_params[idx], key, w, key["join"])
					else:
						self.__append_where(lst_where_text[idx], lst_where_params[idx], key, w, tbl_name)
			else:
				if key["role"] == "join":
					self.__append_where(lst_where_text[0], lst_where_params[0], key, dict_select, key["join"])
				else:
					self.__append_where(lst_where_text[0], lst_where_params[0], key, dict_select, tbl_name)
			# ORDER情報の追加
			if key["key"] in sort_keys:
				sort_key_infos[key["key"]] = key
		# 固定WHERE句は列条件の後ろへ追加する（バインド値を持たない）
		for w in lst_where_text:
			w.extend(fixed_where)
		result.append("select {0} from `{1}`".format(
			", ".join(select_text),
			tbl_id
		))
		if tbl_name != tbl_id:
			result.append(" as `{0}`".format(tbl_name))
		for tbl in def_tbl.get("join", []):
			join_name = tbl.get("name", "")
			if not join_name in join_tbl:
				continue
			on_text = []
			base_tbl = tbl.get("base_table", tbl_name)
			join_key = tbl.get("join_key", "").split(",")
			join_type = tbl.get("join_type", "inner")
			on_key = tbl.get("on_key", "").split(",")
			for i, k in enumerate(join_key):
				on_text.append("`{0}`.`{1}` = `{2}`.`{3}`".format(base_tbl, k, join_name, on_key[i]))
			result.append(" {0} join `{1}` as `{2}` on {3}".format(
				join_type,
				tbl.get("id", ""),
				join_name,
				" and ".join(on_text)
			))
		where_text = []
		# SQL文へ現れる順序どおりにバインド値を並べる
		for lst, prm in zip(lst_where_text, lst_where_params):
			if len(lst) > 0:
				where_text.append(" and ".join(lst))
				params.extend(prm)
		if len(where_text) > 0:
			result.append(" where ({0})".format(
				") or (".join(where_text)
			))
		if not sort_order:
			orders = def_tbl.get("sort", [])
		else:
			orders = []
			for s in sort_order:
				order = {
					"key": s["key"],
					"order": s["order"]
				}
				info = sort_key_infos[s["key"]]
				if info["role"] == "join":
					order["tbl"] = info["join"]
					order["key"] = info["get_key"]
				orders.append(order)
		for key in orders:
			if key.get("key", "") == "":
				continue
			# ORDER句への追加
			if not key["key"] in lst_exclude:
				if key.get("tbl") is not None:
					sort_text.append("`{0}`.`{1}` {2}".format(key["tbl"], key["key"], key.get("order", "")))
				else:
					sort_text.append("`{0}`.`{1}` {2}".format(tbl_name, key["key"], key.get("order", "")))
			result.append(" order by {0}".format(
				", ".join(sort_text)
			))
		if for_update:
			result.append(" for update")
		result.append(";")
		return "".join(result), params

	def get_distinct_sql(self, tbl_id, lst_select=[], dict_select={}):
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

		Returns
		-------
		(sql, params) : (string, list)
			SQL文とバインド値の組。テーブル未定義の場合は(None, [])を返す。
		"""
		if not tbl_id in self.tables.keys():
			return None, []
		def_tbl = self.tables[tbl_id]
		tbl_name = def_tbl.get("name", tbl_id)
		select_text = []
		where_text = []
		where_params = []
		result = []
		for key in def_tbl.get("column", []):
			if key.get("key", "") == "":
				continue
			# SELECT句への追加
			if key["key"] in lst_select:
				if key["role"] != "join":
					select_text.append("`{0}`.`{1}`".format(tbl_name, key["key"]))
			if key["role"] != "join":
				self.__append_where(where_text, where_params, key, dict_select, tbl_name)
		result.append("select distinct {0} from {1}".format(
			", ".join(select_text),
			tbl_id
		))
		if tbl_name != tbl_id:
			result.append(" as {0}".format(tbl_name))
		if len(where_text) > 0:
			result.append(" where {0}".format(
				" and ".join(where_text)
			))
		result.append(";")
		return "".join(result), where_params

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
			key_params = []
			update_params = []
			for key in def_tbl.get("column", []):
				if "join" in key["role"] or key.get("key", "") == "":
					continue
				value = self.get_value(key, record)
				if "key" in key["role"]:
					key_text.append("`{0}` = {1}".format(key["key"], self.placeholder))
					key_params.append(value)
				elif "up" in key["role"]:
					update_text.append("`{0}` = {1}".format(key["key"], self.placeholder))
					update_params.append(value)
				col_text.append("`{0}`".format(key["key"]))
				set_text.append(self.placeholder)
				set_params.append(value)
			sql = ["insert into {0} ({1}) values ({2})".format(
				tbl_id,
				", ".join(col_text),
				", ".join(set_text)
			)]
			params = list(set_params)
			if is_upsert:
				# 重複時は更新可能な列（role="up"）を更新する。キー列だけを
				# 更新対象にすると同じ値で上書きするだけになりUPSERTが機能しない。
				# up列を持たないテーブルではキー列を対象にしてSQLとして成立させる。
				duplicate_text = update_text if update_text else key_text
				duplicate_params = update_params if update_text else key_params
				sql.append(" on duplicate key update ")
				sql.append(", ".join(duplicate_text))
				params.extend(duplicate_params)
			sql.append(";")
			result.append(("".join(sql), params))
		return result

	def get_increment_sql(self, tbl_id, dict_increment, dict_where={}):
		"""
		指定した列を加算するUPDATE文の生成処理

		「読み取り→アプリ側で加算→書き込み」では、同じ行への更新が並行した
		ときにロストアップデートが起きる（server.pyはnum_processes=0で
		CPU数ぶんのプロセスを起動するため、別プロセス間で並行し得る）。
		加算はDB側の式で行い、値のみをバインドする。

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_increment : dict
			加算する列と加算値。{ "key1" : 1 }のように指定する。
		dict_where : dict, default {}
			更新対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。

		Returns
		-------
		(sql, params) : (string, list)
			SQL文とバインド値の組。テーブル未定義、または加算対象の列が
			存在しない場合は(None, [])を返す。
		"""
		if not tbl_id in self.tables.keys():
			return None, []
		def_tbl = self.tables[tbl_id]
		set_text = []
		set_params = []
		where_text = []
		where_params = []
		for key in def_tbl.get("column", []):
			k = key.get("key", "")
			if k == "":
				continue
			self.__append_where(where_text, where_params, key, dict_where)
			if not k in dict_increment.keys():
				continue
			# NULLは0として扱う（カラム追加直後で未初期化の行を考慮する）
			set_text.append("`{0}` = coalesce(`{0}`, 0) + {1}".format(k, self.placeholder))
			set_params.append(dict_increment[k])
		# 加算する項目が存在しない場合はNoneを返す
		if len(set_text) == 0:
			return None, []
		result = []
		result.append("update `{0}` set {1}".format(
			tbl_id,
			", ".join(set_text)
		))
		# SET句のバインド値がWHERE句より先に現れる
		params = set_params + where_params
		if len(where_text) > 0:
			result.append(" where {0}".format(
				" and ".join(where_text)
			))
		result.append(";")
		return "".join(result), params

	def get_update_sql(self, tbl_id, dict_update, dict_where={}):
		"""
		レコード更新SQL生成処理

		Parameters
		dict_update : dict
			更新情報辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		dict_where : dict, default {}
			更新対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		----------

		Returns
		-------
		(sql, params) : (string, list)
			SQL文とバインド値の組。テーブル未定義、または更新する項目が
			存在しない場合は(None, [])を返す。
		"""
		if not tbl_id in self.tables.keys():
			return None, []
		def_tbl = self.tables[tbl_id]
		set_text = []
		set_params = []
		where_text = []
		where_params = []
		for key in def_tbl.get("column", []):
			k = key.get("key", "")
			if k == "":
				continue
			self.__append_where(where_text, where_params, key, dict_where)
			if not "up" in key["role"]:
				continue
			if not k in dict_update.keys():
				continue
			set_text.append("`{0}` = {1}".format(k, self.placeholder))
			set_params.append(self.get_value(key, dict_update))
		# 更新する項目が存在しない場合はNoneを返す
		if len(set_text) == 0:
			return None, []
		result = []
		result.append("update `{0}` set {1}".format(
			tbl_id,
			", ".join(set_text)
		))
		# SET句のバインド値がWHERE句より先に現れる
		params = set_params + where_params
		if len(where_text) > 0:
			result.append(" where {0}".format(
				" and ".join(where_text)
			))
		result.append(";")
		return "".join(result), params

	def get_delete_sql(self, tbl_id, lst_delete=[]):
		"""
		レコード削除SQL生成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_delete : list of dict, default []
			削除対象条件配列。
			[{ "key1" : "value1", "key2" : "value2"}]のようにキー項目と値を設定する。

		Returns
		-------
		list of (sql, params)
			削除条件ごとのSQL文とバインド値の組の配列。
			テーブル未定義の場合はNoneを返す。
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		if len(lst_delete) == 0:
			return [("delete from `{0}`;".format(tbl_id), [])]
		result = []
		for record in lst_delete:
			where_text = []
			where_params = []
			for key in def_tbl.get("column", []):
				if key.get("key", "") == "":
					continue
				self.__append_where(where_text, where_params, key, record)
			sql = ["delete from `{0}`".format(tbl_id)]
			if len(where_text) > 0:
				sql.append(" where {0}".format(
					" and ".join(where_text)
				))
			sql.append(";")
			result.append(("".join(sql), where_params))
		return result

	def get_value(self, key, record):
		"""
		バインドする値の取得処理

		レコードに値がない場合はテーブル定義のdefaultを使う。
		空文字はNULLとして扱う（従来のset_value_textと同じ判定）。

		bool列に文字列（テーブル定義XMLのdefault="true"等）が渡された場合は
		Pythonのboolへ正規化する。値の埋め込み時はSQLのtrue/falseリテラルに
		なっていたため、バインドでも同じ真偽値として扱う必要がある
		（文字列のままだとTEXTとして保存され、比較時の型変換もずれる）。
		"""
		val = record.get(key["key"], key.get("default", None))
		if val == "" or val is None:
			return None
		return self.normalize_bool(key, val)

	def normalize_bool(self, key, val):
		"""
		bool列の値の正規化処理

		bool列に真偽値以外（文字列等）が渡された場合はPythonのboolへ変換する
		（"true" / "1" を真とする）。bool列以外・None・真偽値はそのまま返す。
		INSERT・UPDATEの値（get_value）とWHERE句の条件（__append_where）で同じ変換を行い、
		保存した値と条件の値が一致するようにする。
		"""
		if key.get("type") != "bool" or val is None or isinstance(val, bool):
			return val
		return str(val).strip().lower() in ("true", "1")

	def __append_where(self, lst_text, lst_params, key, record, name=""):
		"""
		WHERE句の条件を1件解釈し、条件文とバインド値を追加する。

		条件文とバインド値を同じ順序で追加するため、SQL文中のプレースホルダと
		バインド値の並びが常に一致する。条件に含まれないキーは何も追加しない。
		"""
		k = key["key"]
		# 条件に含まれていないなら何も追加しない
		if not k in record.keys():
			return
		tbl_name = "`{0}`.".format(name)
		if tbl_name == "``.":
			tbl_name = ""
		val = record.get(k, None)
		if key.get("get_key"):
			k = key["get_key"]
		# bool列の条件は保存時（get_value）と同じく真偽値へ正規化する。値の埋め込み時は
		# SQLのtrue/falseリテラルとして比較していたため、文字列のままバインドすると
		# "false"等の条件が一致しなくなる（演算子指定・リストの値も同様）
		if isinstance(val, dict):
			self.__append_condition(lst_text, lst_params, tbl_name, k,
				self.normalize_where_value(key, val.get("value")), val.get("operator", "="))
		else:
			self.__append_condition(lst_text, lst_params, tbl_name, k,
				self.normalize_where_value(key, val))

	def normalize_where_value(self, key, val):
		"""
		WHERE句の条件値の正規化処理（bool列の値を真偽値へ変換する。リストは要素ごと）
		"""
		if isinstance(val, list):
			return [self.normalize_bool(key, v) for v in val]
		return self.normalize_bool(key, val)

	def __append_condition(self, lst_text, lst_params, tbl_name, key, val, operator="="):
		"""
		比較条件をプレースホルダで組み立て、条件文とバインド値を追加する。
		"""
		if val is None:
			lst_text.append("{0}`{1}` is null".format(tbl_name, key))
			return
		if isinstance(val, list):
			holders = [self.placeholder for v in val]
			if operator == "between":
				lst_text.append("{0}`{1}` between {2} and {3}".format(
					tbl_name, key, self.placeholder, self.placeholder))
				lst_params.extend(val[0:2])
				return
			in_sql = "not in" if operator == "not" else "in"
			lst_text.append("{0}`{1}` {2} ({3})".format(
				tbl_name, key, in_sql, ",".join(holders)))
			lst_params.extend(val)
			return
		# LIKEも含め、比較する値はすべてバインドする
		lst_text.append("{0}`{1}` {2} {3}".format(
			tbl_name, key, operator, self.placeholder))
		lst_params.append(val)
