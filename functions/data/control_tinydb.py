# -*- coding: utf-8 -*-

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
			固定WHERE区配列（TinyDBでは未サポート）。
		"""
		if not tbl_id in self.tables.keys():
			print("指定されたテーブル「{0}」は該当のデータベースに存在しません。".format(tbl_id))
			return []
		
		table = self.db.table(tbl_id)
		if not dict_select:
			results = table.all()
		else:
			q = Query()
			cond = None
			for k, v in dict_select.items():
				if cond is None:
					cond = (q[k] == v)
				else:
					cond &= (q[k] == v)
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
		q = Query()
		cond = None
		for k, v in dict_where.items():
			if cond is None:
				cond = (q[k] == v)
			else:
				cond &= (q[k] == v)
		
		if cond:
			table.update(dict_update, cond)
		else:
			table.update(dict_update)

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
			q = Query()
			for record in lst_delete:
				cond = None
				for k, v in record.items():
					if cond is None:
						cond = (q[k] == v)
					else:
						cond &= (q[k] == v)
				if cond:
					table.remove(cond)

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

	def escape(self, col_type, val):
		return val
