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

if __name__ == "__main__":
    import unittest
    unittest.main()
