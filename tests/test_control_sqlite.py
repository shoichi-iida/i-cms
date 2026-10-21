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

	def test_increment(self):
		"""加算処理: NULLは0扱い、WHERE条件に一致する行のみ加算する

		テスト用テーブル定義に整数列がないため、数値を保持できるadmin列
		（boolean宣言＝SQLiteではNUMERIC affinity）へ生SQLで初期値を入れて検証する。
		insert()経由だとbool列の値がTrue/Falseへ正規化されるため使わない。
		"""
		self.ctrl_base.exec_sql(
			"insert into tbl_account (id, password, name, admin)"
			" values ('test1', 'hoge', 'Pon!', 3);")
		self.ctrl_base.exec_sql(
			"insert into tbl_account (id, password, name, admin)"
			" values ('test2', 'hoge', 'Pon!', 0);")
		# admin未指定＝NULLの行
		self.ctrl_base.exec_sql(
			"insert into tbl_account (id, password, name) values ('test3', 'hoge', 'Pon!');")

		self.ctrl_base.increment("tbl_account", {"admin": 1}, {"id": "test1"})
		self.assertEqual(self.admin_of("test1"), 4)
		# 条件外の行は変化しない
		self.assertEqual(self.admin_of("test2"), 0)

		# NULLはcoalesceで0として扱う
		self.assertIsNone(self.admin_of("test3"))
		self.ctrl_base.increment("tbl_account", {"admin": 2}, {"id": "test3"})
		self.assertEqual(self.admin_of("test3"), 2)

	def test_bool_string_conditions(self):
		"""bool列の条件の文字列（"false"・"1"等）は保存時と同じく真偽値として比較する（WHERE・UPDATE・DELETE）"""
		self.ctrl_base.insert("tbl_auth", [
			{"id": "a", "function": "f", "value": "false"},
			{"id": "b", "function": "f", "value": "true"},
		])
		self.assertEqual([r["id"] for r in self.ctrl_base.select("tbl_auth", {"value": "false"})], ["a"])
		self.assertEqual([r["id"] for r in self.ctrl_base.select("tbl_auth", {"value": "1"})], ["b"])
		# 演算子指定・リストの値も正規化する
		self.assertEqual(
			[r["id"] for r in self.ctrl_base.select("tbl_auth", {"value": {"value": "true", "operator": "="}})], ["b"])
		self.assertEqual(
			sorted(r["id"] for r in self.ctrl_base.select("tbl_auth", {"value": ["false", "true"]})), ["a", "b"])
		self.ctrl_base.update("tbl_auth", {"function": "g"}, {"value": "false"})
		self.assertEqual(
			[(r["id"], r["function"]) for r in self.ctrl_base.select("tbl_auth")], [("a", "g"), ("b", "f")])
		self.ctrl_base.delete("tbl_auth", [{"value": "false"}])
		self.assertEqual([r["id"] for r in self.ctrl_base.select("tbl_auth")], ["b"])

	def admin_of(self, account_id):
		return self.ctrl_base.exec_sql(
			"SELECT admin FROM tbl_account WHERE id = ?;", [account_id])[0]["admin"]

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

	def test_create_table_autoincrement(self):
		"""採番列（autoincrement="true"）はSQLiteのAUTOINCREMENT（列制約）になる"""
		self.assertEqual(
			self.ctrl_base.get_create_table_sql("tbl_sequence"),
			"create table if not exists tbl_sequence (id integer primary key autoincrement, name text);")

	def test_autoincrement_does_not_reuse_deleted_max_id(self):
		"""採番列は最大の番号の行を物理削除しても、同じ番号を再び採番しない（issue #260）

		AUTOINCREMENTでない整数の主キーはrowidの別名になり「その時点の最大値 + 1」で
		採番されるため、最大の番号の行を消すと同じ番号がもう一度使われる。
		"""
		self.ctrl_base.create_table("tbl_sequence")
		ids = [self.ctrl_base.insert("tbl_sequence", [{"name": name}])[0]["id"]
			for name in ["a", "b", "c"]]
		self.assertEqual(ids, [1, 2, 3])
		self.ctrl_base.delete("tbl_sequence", [{"id": 3}])
		self.assertEqual(self.ctrl_base.insert("tbl_sequence", [{"name": "d"}])[0]["id"], 4)
		# 全行を削除しても番号は戻らない
		self.ctrl_base.delete("tbl_sequence")
		self.assertEqual(self.ctrl_base.insert("tbl_sequence", [{"name": "e"}])[0]["id"], 5)
