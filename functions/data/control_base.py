# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

class ControlBase(metaclass=ABCMeta):
	"""
	DB制御基底クラス
	"""
	def __init__(self):
		"""
		コンストラクタ
		"""
		self.tables = {}
		self.text_type = ["text", "datetime"]

	def __del__(self):
		"""
		デストラクタ
		"""
		pass

	@abstractmethod
	def begin(self):
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
	def delete(self, tbl_id, lst_delete=[]):
		pass

	def get_create_table_sql(self, tbl_id):
		"""
		テーブル作成SQL生成処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		col_text = []
		key_text = []
		for key in def_tbl.get("column", []):
			if "join" in key["role"] or key.get("key", "") == "":
				continue
			if "key" in key["role"]:
				key_text.append(key["key"])
			col_type = "varchar"
			if key["type"] == "int":
				col_type = "integer"
			elif key["type"] == "float":
				col_type = "decimal"
			elif key["type"] == "bool":
				col_type = "boolean"
			elif key["type"] == "datetime":
				col_type = "datetime"
			col_text.append("{0} {1}".format(key["key"], col_type))
		if len(key_text) > 0:
			col_text.append("primary key({0})".format(", ".join(key_text)))
		return "create table if not exists {0} ({1});".format(
			tbl_id,
			", ".join(col_text)
		)

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

	def get_select_sql(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[]):
		"""
		レコード取得SQL生成処理

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
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		tbl_name = def_tbl.get("name", tbl_id)
		select_text = []
		join_tbl = []
		where_text = []
		sort_text = []
		result = []
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
			if key["role"] == "join":
				where_text.append(self.__set_where_text(key, dict_select, key["join"]))
			else:
				where_text.append(self.__set_where_text(key, dict_select, tbl_name))
		where_text.extend(fixed_where)
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
			on_key = tbl.get("on_key", "").split(",")
			for i, k in enumerate(join_key):
				on_text.append("`{0}`.`{1}` = `{2}`.`{3}`".format(base_tbl, k, join_name, on_key[i]))
			result.append(" inner join `{0}` as `{1}` on {2}".format(
				tbl.get("id", ""),
				join_name,
				" and ".join(on_text)
			))
		where_text = [t for t in where_text if t is not None]
		if len(where_text) > 0:
			result.append(" where {0}".format(
				" and ".join(where_text)
			))
		for key in def_tbl.get("sort", []):
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
		result.append(";")
		return "".join(result)

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
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		tbl_name = def_tbl.get("name", tbl_id)
		select_text = []
		where_text = []
		result = []
		for key in def_tbl.get("column", []):
			if key.get("key", "") == "":
				continue
			# SELECT句への追加
			if key["key"] in lst_select:
				if key["role"] != "join":
					select_text.append("`{0}`.`{1}`".format(tbl_name, key["key"]))
			if key["role"] != "join":
				where_text.append(self.__set_where_text(key, dict_select, tbl_name))
		result.append("select distinct {0} from {1}".format(
			", ".join(select_text),
			tbl_id
		))
		if tbl_name != tbl_id:
			result.append(" as {0}".format(tbl_name))
		where_text = [t for t in where_text if t is not None]
		if len(where_text) > 0:
			result.append(" where {0}".format(
				" and ".join(where_text)
			))
		result.append(";")
		return "".join(result)

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
			for key in def_tbl.get("column", []):
				if "join" in key["role"] or key.get("key", "") == "":
					continue
				if "key" in key["role"]:
					key_text.append(f"`{key['key']}` = {self.__set_value_text(key, record)}")
				col_text.append(f"`{key['key']}`")
				set_text.append(self.__set_value_text(key, record))
			sql = ["insert into {0} ({1}) values ({2})".format(
				tbl_id,
				", ".join(col_text),
				", ".join(set_text)
			)]
			if is_upsert:
				sql.append(" on duplicate key update ")
				sql.append(", ".join(key_text))
			sql.append(";")
			result.append("".join(sql))
		return result

	def get_update_sql(self, tbl_id, dict_update, dict_where={}):
		"""
		レコード更新SQL生成処理

		Parameters
		dict_update : dict
			更新情報辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		dict_where : dict, default []
			更新対象条件辞書。
			{ "key1" : "value1", "key2" : "value2"}のようにキー項目と値を設定する。
		----------
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		set_text = []
		where_text = []
		for key in def_tbl.get("column", []):
			k = key.get("key", "")
			if k == "":
				continue
			where_text.append(self.__set_where_text(key, dict_where))
			if not "up" in key["role"]:
				continue
			set_text.append(self.__set_update_text(key, dict_update))
		# 更新する項目が存在しない場合はNoneを返す
		set_text = [t for t in set_text if t is not None]
		if len(set_text) == 0:
			return None
		result = []
		result.append("update `{0}` set {1}".format(
			tbl_id,
			", ".join(set_text)
		))
		where_text = [t for t in where_text if t is not None]
		if len(where_text) > 0:
			result.append(" where {0}".format(
				" and ".join(where_text)
			))
		result.append(";")
		return "".join(result)

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
		"""
		if not tbl_id in self.tables.keys():
			return None
		def_tbl = self.tables[tbl_id]
		result = []
		for record in lst_delete:
			where_text = []
			for key in def_tbl.get("column", []):
				if key.get("key", "") == "":
					continue
				where_text.append(self.__set_where_text(key, record))
			result.append("delete from `{0}`".format(
				tbl_id
			))
			where_text = [t for t in where_text if t is not None]
			if len(where_text) > 0:
				result.append(" where {0}".format(
					" and ".join(where_text)
				))
			result.append(";")
		if len(lst_delete) == 0:
			return "delete from `{0}`;".format(tbl_id)
		else:
			return "".join(result)

	def __set_value_text(self, key, record):
		val = record.get(key["key"], key.get("default", None))
		if val == "" or val is None:
			return "null"
		else:
			return self.escape(key["type"], val)
	
	def __set_update_text(self, key, record):
		k = key["key"]
		if not k in record.keys():
			return None
		return "`{0}` = {1}".format(k, self.__set_value_text(key, record))

	def __set_where_text(self, key, record, name=""):
		k = key["key"]
		# 条件に含まれていないなら空を返却
		if not k in record.keys():
			return None
		tbl_name = "`{0}`.".format(name)
		if tbl_name == "``.":
			tbl_name = ""
		val = record.get(k, None)
		if val is None:
			return "{0}`{1}` is null".format(tbl_name, k)
		else:
			return "{0}`{1}` = {2}".format(tbl_name, k, self.escape(key["type"], val))

	@abstractmethod
	def escape(self, col_type, val):
		if not col_type in self.text_type:
			return "{0}".format(val)
		return "'{0}'".format(val)
