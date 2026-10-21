#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from functions.define.base_define import BaseDefine
from functions.data.control_tinydb import ControlTinyDB

class TestControlTinyDB(TestCase):
	def setUp(self):
		self.ctrl_base = ControlTinyDB()
		# テスト用のテーブル定義を読み込む（SQLite用と共通）
		def_tbl = BaseDefine("tests/data/test_tables.xml").dict
		for tbl in def_tbl.values():
			self.ctrl_base.tables[tbl["id"]] = tbl
		self.ctrl_base.drop_tables()

	def test_crud(self):
		# tbl_account にデータがないことを確認
		result = self.ctrl_base.select("tbl_account")
		self.assertEqual(len(result), 0)

		# Insert
		data = {"id": "test1", "password": "hoge", "name": "Pon!", "admin": "0"}
		self.ctrl_base.insert("tbl_account", [data])
		
		# Select
		result = self.ctrl_base.select("tbl_account", {"id": "test1"})
		self.assertEqual(len(result), 1)
		self.assertEqual(result[0]["id"], "test1")

		# Update
		self.ctrl_base.update("tbl_account", {"name": "Updated!"}, {"id": "test1"})
		result = self.ctrl_base.select("tbl_account", {"id": "test1"})
		self.assertEqual(result[0]["name"], "Updated!")

		# Upsert (insert)
		upsert_data = {"id": "test2", "password": "pass", "name": "New", "admin": "1"}
		self.ctrl_base.insert("tbl_account", [upsert_data], is_upsert=True)
		result = self.ctrl_base.select("tbl_account", {"id": "test2"})
		self.assertEqual(len(result), 1)

		# Upsert (update)
		upsert_data_update = {"id": "test2", "name": "Upserted!"}
		self.ctrl_base.insert("tbl_account", [upsert_data_update], is_upsert=True)
		result = self.ctrl_base.select("tbl_account", {"id": "test2"})
		self.assertEqual(len(result), 1)
		self.assertEqual(result[0]["name"], "Upserted!")
		self.assertEqual(result[0]["password"], "pass") # 既存の値が残っているか

		# Delete
		self.ctrl_base.delete("tbl_account", [{"id": "test1"}])
		result = self.ctrl_base.select("tbl_account", {"id": "test1"})
		self.assertEqual(len(result), 0)

	def test_increment(self):
		"""加算処理: 未設定は0扱い、WHERE条件に一致する行のみ加算する"""
		self.ctrl_base.insert("tbl_account", [
			{"id": "a", "name": "A"},                      # 未設定（Noneでもない）
			{"id": "b", "name": "B", "admin": 5},
			{"id": "c", "name": "C", "admin": None},       # None
		])

		self.ctrl_base.increment("tbl_account", {"admin": 1}, {"id": "a"})
		self.assertEqual(self.ctrl_base.select("tbl_account", {"id": "a"})[0]["admin"], 1)

		# 既存値へ加算され、条件外の行は変化しない
		self.ctrl_base.increment("tbl_account", {"admin": 2}, {"id": "b"})
		self.assertEqual(self.ctrl_base.select("tbl_account", {"id": "b"})[0]["admin"], 7)
		self.assertEqual(self.ctrl_base.select("tbl_account", {"id": "a"})[0]["admin"], 1)

		# NoneはSQL実装のcoalesceと同じく0扱い
		self.ctrl_base.increment("tbl_account", {"admin": 1}, {"id": "c"})
		self.assertEqual(self.ctrl_base.select("tbl_account", {"id": "c"})[0]["admin"], 1)

	def test_distinct(self):
		self.ctrl_base.insert("tbl_account", [
			{"id": "a", "name": "Same", "admin": "0"},
			{"id": "b", "name": "Same", "admin": "0"},
			{"id": "c", "name": "Diff", "admin": "1"},
		])
		
		result = self.ctrl_base.distinct("tbl_account", ["name"])
		self.assertEqual(len(result), 2)
		names = sorted([r["name"] for r in result])
		self.assertEqual(names, ["Diff", "Same"])

	def test_select_list_condition_is_in(self):
		"""リスト指定はIN条件として扱う（SQLite・MySQL実装に合わせる）

		値の一致比較のままでは常に0件になり、複数IDでの絞り込みを行うAPI
		（PRJ001・IAE001・ORG004等）が結果を返さない。
		"""
		self.ctrl_base.insert("tbl_account", [
			{"id": "a", "password": "p", "name": "A", "admin": "0"},
			{"id": "b", "password": "p", "name": "B", "admin": "0"},
			{"id": "c", "password": "p", "name": "C", "admin": "1"},
		])

		result = self.ctrl_base.select("tbl_account", {"id": ["a", "c"]})
		self.assertEqual(sorted(r["id"] for r in result), ["a", "c"])

		# 他の条件とAND結合できる
		result = self.ctrl_base.select(
			"tbl_account", {"id": ["a", "c"], "admin": "1"})
		self.assertEqual([r["id"] for r in result], ["c"])

		# 空リストは該当なし
		self.assertEqual(
			self.ctrl_base.select("tbl_account", {"id": []}), [])

		# スカラー指定は従来どおり一致比較
		result = self.ctrl_base.select("tbl_account", {"id": "b"})
		self.assertEqual([r["id"] for r in result], ["b"])

		# IN展開の対象はlistのみ（ControlBase.__append_conditionと揃える）。
		# tuple・setはSQL実装でもIN展開されないため、ここでも展開しない
		self.assertEqual(
			self.ctrl_base.select("tbl_account", {"id": ("a", "c")}), [])

if __name__ == "__main__":
    import unittest
    unittest.main()
