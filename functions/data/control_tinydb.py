# -*- coding: utf-8 -*-

import operator
import re
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage
from functions.data.control_base import ControlBase

class ControlTinyDB(ControlBase):
	"""
	TinyDB制御クラス
	"""
	def __init__(self, db_path=None):
		"""
		コンストラクタ

		Parameters
		----------
		db_path : string
			DB保存パス
		"""
		super().__init__()
		self.make_db_dir(db_path)
		# DB保存パス（インメモリの場合はNone）。データ移行（MigrateData）でのバックアップに使用する
		self.db_path = db_path
		if db_path is None:
			self.db = TinyDB(storage = MemoryStorage)
		else:
			self.db = TinyDB(db_path, sort_keys=True)

	def __del__(self):
		"""
		デストラクタ
		"""
		super().__del__()
		self.db.close()

	def begin(self, serialize=None):
		"""
		トランザクション開始処理
		TinyDBにトランザクション機能は存在しないが、I/F互換性のために定義。
		書き込みの直列化（serialize）も未サポート。
		"""
		pass

	def commit(self):
		"""
		トランザクションコミット処理
		TinyDBにトランザクション機能は存在しないが、I/F互換性のために定義。
		"""
		pass

	def rollback(self):
		"""
		トランザクションロールバック処理
		TinyDBにトランザクション機能は存在しないが、I/F互換性のために定義。
		"""
		pass

	def create_table(self, tbl_id):
		"""
		テーブル作成処理
		TinyDBでは明示的なテーブル作成は不要だが、I/F互換性のために定義。
		"""
		pass

	def drop_table(self, tbl_id):
		"""
		テーブル削除処理
		
		Parameters
		----------
		tbl_id : string
			テーブルID
		"""
		self.db.drop_table(tbl_id)

	def select(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[]):
		"""
		レコード取得処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_select : dict, default {}
			取得対象条件辞書。
		lst_exclude : list of string, default []
			取得除外対象配列。
		fixed_where : list of string default []
			固定WHERE句配列（TinyDBでは未サポート）。
		"""
		if not tbl_id in self.tables.keys():
			print("指定されたテーブル「{0}」は該当のデータベースに存在しません。".format(tbl_id))
			return []

		def_tbl = self.tables[tbl_id]
		
		table = self.db.table(tbl_id)
		if not dict_select:
			results = table.all()
		else:
			q = Query()
			cond = None
			for key in def_tbl.get("column", []):
				if key.get("key", "") == "" or not key["key"] in dict_select.keys():
					continue
				c = None
				v = dict_select[key["key"]]
				if key["role"] == "join":
					continue
				elif key["role"] == "like":
					k = key["base_key"]
					if v.startswith("%") and v.endswith("%"):
						c = (q[k].search(v[1:-1]))
					elif not v.startswith("%") and v.endswith("%"):
						c = (q[k].matches(f"^{v[:-1]}"))
					elif v.startswith("%") and not v.endswith("%"):
						c = (q[k].matches(f"{v[1:]}$"))
				else:
					k = key["key"]
					c = self.get_column_condition(q[k], self.normalize_where_value(key, dict_select[k]))
				if cond is None:
					cond = c
				else:
					cond &= c
			results = table.search(cond)
		
		if lst_exclude:
			for row in results:
				for key in lst_exclude:
					if key in row:
						del row[key]
		return results

	def distinct(self, tbl_id, lst_select=[], dict_select={}):
		"""
		重複排除取得処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_select : list of string, default []
			取得対象列配列。
		dict_select : dict, default {}
			取得対象条件辞書。
		"""
		results = self.select(tbl_id, dict_select)
		if not lst_select:
			return results
		
		distinct_results = []
		seen = set()
		for row in results:
			filtered_row = {k: row[k] for k in lst_select if k in row}
			row_tuple = tuple(sorted(filtered_row.items()))
			if row_tuple not in seen:
				seen.add(row_tuple)
				distinct_results.append(filtered_row)
		return distinct_results

	def insert(self, tbl_id, lst_insert, is_upsert=False):
		"""
		レコード挿入処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_insert : list of dict
			挿入情報配列。
		is_upsert : bool, default False
			UPSERT（更新または挿入）を行うかどうか。
		"""
		if not tbl_id in self.tables.keys():
			print("指定されたテーブル「{0}」は該当のデータベースに存在しません。".format(tbl_id))
			return
		
		table = self.db.table(tbl_id)
		lst_insert = [self.normalize_record(tbl_id, record) for record in lst_insert]
		if is_upsert:
			def_tbl = self.tables[tbl_id]
			keys = [col["key"] for col in def_tbl.get("column", []) if "key" in col.get("role", "")]
			
			for record in lst_insert:
				if keys:
					q = Query()
					cond = None
					for k in keys:
						if k in record:
							if cond is None:
								cond = (q[k] == record[k])
							else:
								cond &= (q[k] == record[k])
					
					if cond and table.search(cond):
						table.update(record, cond)
						continue
				table.insert(record)
		else:
			table.insert_multiple(lst_insert)

	def update(self, tbl_id, dict_update, dict_where={}):
		"""
		レコード更新処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_update : dict
			更新情報辞書。
		dict_where : dict
			更新対象条件辞書。
		"""
		if not tbl_id in self.tables.keys():
			print("指定されたテーブル「{0}」は該当のデータベースに存在しません。".format(tbl_id))
			return
		
		table = self.db.table(tbl_id)
		dict_update = self.normalize_record(tbl_id, dict_update)
		cond = self.get_condition(tbl_id, dict_where)
		
		if cond:
			table.update(dict_update, cond)
		else:
			table.update(dict_update)

	def increment(self, tbl_id, dict_increment, dict_where={}):
		"""
		指定した列の加算処理

		TinyDBのupdateへ関数を渡し、読み込んだ値ではなく保存時の値を基準に
		加算する（SQL実装と同じくアプリ側で加算した結果を書き戻さない）。

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_increment : dict
			加算する列と加算値。{ "key1" : 1 }のように指定する。
		dict_where : dict, default {}
			更新対象条件辞書。
		"""
		if not tbl_id in self.tables.keys():
			print("指定されたテーブル「{0}」は該当のデータベースに存在しません。".format(tbl_id))
			return

		table = self.db.table(tbl_id)
		cond = self.get_condition(tbl_id, dict_where)

		def add_values(doc):
			for k, v in dict_increment.items():
				# NULL・未設定は0として扱う（SQL実装のcoalesceと揃える）
				doc[k] = (doc.get(k) or 0) + v

		if cond:
			table.update(add_values, cond)
		else:
			table.update(add_values)

	def delete(self, tbl_id, lst_delete=[]):
		"""
		レコード削除処理

		Parameters
		----------
		tbl_id : string
			テーブルID
		lst_delete : list of dict, default []
			削除対象条件配列。
		"""
		if not tbl_id in self.tables.keys():
			print("指定されたテーブル「{0}」は該当のデータベースに存在しません。".format(tbl_id))
			return
		
		table = self.db.table(tbl_id)
		if not lst_delete:
			table.truncate()
		else:
			for record in lst_delete:
				cond = self.get_condition(tbl_id, record)
				if cond:
					table.remove(cond)

	def normalize_value(self, key, val):
		"""
		値を列定義の型へそろえる処理

		TinyDBは保存した値をそのまま保持し、条件は型まで一致する必要がある（0 と "0" は
		一致しない）。SQL系のDBは列の型へ変換して保存・比較するため、同じ結果になるよう、
		書き込む値と条件の値を列定義の型へそろえる
		（int: 整数の文字列を整数へ、text: 数値を文字列へ、bool: ControlBase.normalize_bool）。
		リストは要素ごとにそろえる。変換できない値・None・その他の型はそのまま返す。

		Parameters
		----------
		key : dict
			列定義（テーブル定義XMLのcolumn）
		val : any
			値

		Returns
		-------
		any
			列定義の型へそろえた値
		"""
		if isinstance(val, list):
			return [self.normalize_value(key, v) for v in val]
		if val is None:
			return None
		col_type = key.get("type")
		if col_type == "bool":
			return self.normalize_bool(key, val)
		if col_type == "int" and isinstance(val, str):
			try:
				return int(val.strip())
			except ValueError:
				return val
		if col_type == "text" and isinstance(val, (int, float)) and not isinstance(val, bool):
			return str(val)
		return val

	# 比較演算子 → 比較関数（ControlBase.__append_conditionがSQLへ展開する演算子と同じ）
	COMPARE_OPERATORS = {
		"=": operator.eq,
		"!=": operator.ne,
		"<>": operator.ne,
		"<": operator.lt,
		"<=": operator.le,
		">": operator.gt,
		">=": operator.ge,
	}

	def get_condition(self, tbl_id, dict_where):
		"""
		条件辞書からTinyDBの検索条件を組み立てる処理（更新・加算・削除）

		各値は列定義の型へそろえ（normalize_where_value）、get_column_conditionで
		SQL実装（ControlBase.__append_condition）と同じく解釈する。

		Parameters
		----------
		tbl_id : string
			テーブルID
		dict_where : dict
			条件辞書

		Returns
		-------
		tinydb.queries.QueryInstance or None
			検索条件。条件がない場合はNone
		"""
		columns = self.get_column_defs(tbl_id)
		q = Query()
		cond = None
		for k, v in dict_where.items():
			if k in columns:
				v = self.normalize_where_value(columns[k], v)
			c = self.get_column_condition(q[k], v)
			cond = c if cond is None else (cond & c)
		return cond

	def get_column_condition(self, field, val):
		"""
		1列の条件をTinyDBの検索条件にする処理

		SQL実装（ControlBase.__append_condition）と同じく解釈する。
		- None: 値がない（is null。キー自体がない行を含む）
		- list: IN（operator="not"ならNOT IN、"between"なら先頭2要素の範囲）
		- {"value": 値, "operator": 演算子}: 演算子で比較（=・!=・<>・<・<=・>・>=・like）
		- それ以外: 一致
		値の一致比較だけでは、リスト・演算子指定の条件が常に不一致となり、SQL実装では
		対象になる行を黙って更新・削除・加算しないため、すべての操作でこの解釈を使う。

		Parameters
		----------
		field : tinydb.queries.Query
			対象列（Query()[列名]）
		val : any
			条件の値

		Returns
		-------
		tinydb.queries.QueryInstance
			検索条件

		Raises
		------
		ValueError
			未対応の演算子の場合（黙って不一致にしない）
		"""
		op = "="
		if isinstance(val, dict):
			op = str(val.get("operator", "=")).strip().lower()
			val = val.get("value")
		if val is None:
			return (~field.exists()) | (field == None)
		if isinstance(val, list):
			if op == "between":
				lower, upper = val[0], val[1]
				return field.test(lambda x: self.compare(operator.ge, x, lower)
								  and self.compare(operator.le, x, upper))
			# SQLのNULLの扱いに合わせる。値がない（NULL・キーなし）行は IN・NOT IN のどちらにも
			# 一致しない（NULLとの比較はNULLになる）。リストにNULLを含む NOT IN は、どの行にも
			# 一致しない（x <> NULL がNULLになるため）。IN のリストのNULLは一致に寄与しない
			values = [v for v in val if v is not None]
			if op == "not":
				if len(values) != len(val):
					return field.test(lambda x: False)
				return field.test(lambda x: x is not None and not x in values)
			# リスト指定はIN条件として扱う。対象をlistに限るのはControlBase.__append_conditionと
			# 揃えるため（tuple・setはどの実装でもIN展開しない）
			return field.test(lambda x: x is not None and x in values)
		if op == "like":
			# LIKEのワイルドカード（%・_）を正規表現へ変換し、値全体と照合する。
			# SQLiteのLIKEと同じく、ASCIIの英字だけ大文字・小文字を区別しない
			# （ASCII以外の文字は区別する）
			pattern = "".join(".*" if ch == "%" else "." if ch == "_" else re.escape(ch)
							  for ch in self.ascii_lower(str(val)))
			return field.test(lambda x: self.like_text(x) is not None
							  and re.fullmatch(pattern, self.ascii_lower(self.like_text(x)), re.S) is not None)
		if not op in self.COMPARE_OPERATORS:
			raise ValueError("TinyDBでは未対応の演算子です: {0}".format(op))
		func = self.COMPARE_OPERATORS[op]
		if func is operator.eq:
			return field == val
		return field.test(lambda x: self.compare(func, x, val))

	# ASCIIの英大文字 → 小文字の変換表（LIKEの照合用。str.lowerはASCII以外も変換するため使わない）
	ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")

	def ascii_lower(self, text):
		"""ASCIIの英大文字だけを小文字にする処理（SQLiteのLIKEと同じ大文字・小文字の扱い）"""
		return text.translate(self.ASCII_LOWER)

	def like_text(self, x):
		"""
		LIKEで照合する文字列の取得処理

		SQLと同じく数値は文字列にして照合する（真偽値はSQLiteの保存値と同じく"1"/"0"）。
		値がない（None）場合はNone（どのパターンにも一致しない）
		"""
		if x is None:
			return None
		if isinstance(x, bool):
			return "1" if x else "0"
		return str(x)

	def compare(self, func, x, val):
		"""
		比較処理（値がない・型が比較できない場合は不一致とする。SQLのNULLとの比較と同じ）
		"""
		if x is None:
			return False
		try:
			return func(x, val)
		except TypeError:
			return False

	def get_column_defs(self, tbl_id):
		"""
		テーブル定義の列（join列・like列を除く）を列名で引ける辞書の取得処理
		"""
		columns = {}
		for key in self.tables.get(tbl_id, {}).get("column", []):
			if key.get("key", "") == "" or "join" in key["role"] or "like" in key["role"]:
				continue
			columns[key["key"]] = key
		return columns

	def normalize_where_value(self, key, val):
		"""
		条件の値を列定義の型へそろえる処理（演算子指定の場合はその値をそろえる）
		"""
		if isinstance(val, dict):
			result = dict(val)
			result["value"] = self.normalize_value(key, val.get("value"))
			return result
		return self.normalize_value(key, val)

	def normalize_record(self, tbl_id, record):
		"""
		レコード（書き込む値・条件）の各値を列定義の型へそろえる処理

		テーブル定義にない列（join列・like列を含む）の値はそのまま返す。

		Parameters
		----------
		tbl_id : string
			テーブルID
		record : dict
			レコード

		Returns
		-------
		dict
			列定義の型へそろえたレコード（新しいdict）
		"""
		columns = self.get_column_defs(tbl_id)
		return {k: self.normalize_value(columns[k], v) if k in columns else v
				for k, v in record.items()}

	def drop_tables(self):
		"""
		全テーブル情報破棄処理
		"""
		self.db.drop_tables()

	def query(self):
		"""
		クエリオブジェクト生成処理（後方互換用）
		"""
		return Query()
