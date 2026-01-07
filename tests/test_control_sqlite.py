#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from functions.define.base_define import BaseDefine
from functions.data.control_sqlite import ControlSqlite

class TestControlSqlite(TestCase):
	def setUp(self):
		self.ctrl_base = ControlSqlite()
		def_tbl = BaseDefine("tests/data/test_tables.xml").dict
		for tbl in def_tbl.values():
			self.ctrl_base.tables[tbl["id"]] = tbl
		self.ctrl_base.drop_table("tbl_account")
		self.ctrl_base.drop_table("tbl_auth")
		self.ctrl_base.create_table("tbl_account")
		self.ctrl_base.create_table("tbl_auth")

	def test_transaction(self):
		result = self.ctrl_base.exec_sql("SELECT COUNT(1) as cnt FROM tbl_account;")
		self.assertEqual(result[0]["cnt"], 0)
		self.ctrl_base.insert("tbl_account", [{"id": "test1", "password": "hoge", "name": "Pon!", "admin": "0"}])
		result = self.ctrl_base.exec_sql("SELECT COUNT(1) as cnt FROM tbl_account;")
		self.assertEqual(result[0]["cnt"], 1)
		result = self.ctrl_base.select("tbl_account")
		self.assertEqual(result, [])
		self.ctrl_base.insert("tbl_auth", [{"id": "test1", "function": "fuck"}])
		result = self.ctrl_base.select("tbl_account")
		self.assertEqual(result, [{"id": "test1", "password": "hoge", "name": "Pon!", "admin": 0, "auth_function": "fuck"}])
		self.ctrl_base.begin()
		self.ctrl_base.delete("tbl_account", [{"id": "test1"}])
		self.ctrl_base.insert("tbl_account", [{"id": "test2", "password": "hoge", "name": "Pon!", "admin": "0"}])
		self.ctrl_base.rollback()
		result = self.ctrl_base.exec_sql("SELECT COUNT(1) as cnt FROM tbl_account;")
		self.assertEqual(result[0]["cnt"], 1)
		result = self.ctrl_base.select("tbl_account")
		self.assertEqual(result, [{"id": "test1", "password": "hoge", "name": "Pon!", "admin": 0, "auth_function": "fuck"}])
