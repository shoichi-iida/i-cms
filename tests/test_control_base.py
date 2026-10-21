#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from functions.define.base_define import BaseDefine
from functions.data.control_base import ControlBase

# テスト用の実装クラス
class MockControl(ControlBase):
	def begin(self): pass
	def commit(self): pass
	def rollback(self): pass
	def create_table(self, tbl_id): pass
	def drop_table(self, tbl_id): pass
	def select(self, tbl_id, dict_select={}, lst_exclude=[], fixed_where=[]): pass
	def distinct(self, tbl_id, lst_select=[], dict_select={}): pass
	def insert(self, tbl_id, lst_insert, is_upsert=False): pass
	def update(self, tbl_id, dict_update, dict_where={}): pass
	def increment(self, tbl_id, dict_increment, dict_where={}): pass
	def delete(self, tbl_id, lst_delete=[]): pass

class TestControlBase(TestCase):
	def setUp(self):
		self.ctrl_base = MockControl()
		def_tbl = BaseDefine("tests/data/test_tables.xml").dict
		for tbl in def_tbl.values():
			self.ctrl_base.tables[tbl["id"]] = tbl

	def test_create_table(self):
		self.assertEqual(self.ctrl_base.get_create_table_sql("tbl_account"), "create table if not exists tbl_account (id text, password text, name text, admin boolean, primary key(id));")

	def test_create_table_autoincrement(self):
		"""採番列（autoincrement="true"）は列制約の主キーになり、primary key句に含めない"""
		self.assertEqual(
			self.ctrl_base.get_create_table_sql("tbl_sequence"),
			"create table if not exists tbl_sequence (id integer primary key auto_increment, name text);")

	def test_create_table_autoincrement_requires_int_key(self):
		"""採番列は整数のキー列にのみ指定できる

		キーでない列・整数でない列に加え、DDLに現れない列（join列・列名なし）への
		誤指定も黙って無視せずValueErrorにする。
		"""
		for column in [
			{"key": "id", "type": "int", "role": "ref,up", "autoincrement": "true"},
			{"key": "id", "type": "text", "role": "ref,key", "autoincrement": "true"},
			{"key": "auth_id", "type": "int", "role": "join", "join": "tbl_auth",
				"get_key": "id", "autoincrement": "true"},
			{"key": "", "type": "int", "role": "ref,key", "autoincrement": "true"},
		]:
			self.ctrl_base.tables["tbl_invalid"] = {"id": "tbl_invalid", "column": [
				{"key": "code", "type": "text", "role": "ref,key"}, column]}
			with self.assertRaises(ValueError, msg=column):
				self.ctrl_base.get_create_table_sql("tbl_invalid")

	def test_create_table_autoincrement_rejects_composite_key(self):
		"""採番列を他のキー列と組み合わせた複合キーにはできない（ValueError）"""
		self.ctrl_base.tables["tbl_invalid"] = {"id": "tbl_invalid", "column": [
			{"key": "id", "type": "int", "role": "ref,key", "autoincrement": "true"},
			{"key": "sub_id", "type": "int", "role": "ref,key"},
		]}
		with self.assertRaises(ValueError):
			self.ctrl_base.get_create_table_sql("tbl_invalid")

	def test_create_table_autoincrement_rejects_multiple_columns(self):
		"""採番列は1テーブルに1列のみ（2列に指定すると主キーが重複するためValueError）"""
		self.ctrl_base.tables["tbl_invalid"] = {"id": "tbl_invalid", "column": [
			{"key": "id", "type": "int", "role": "ref,key", "autoincrement": "true"},
			{"key": "sub_id", "type": "int", "role": "ref,key", "autoincrement": "true"},
		]}
		with self.assertRaises(ValueError):
			self.ctrl_base.get_create_table_sql("tbl_invalid")

	def test_drop_table(self):
		self.assertEqual(self.ctrl_base.get_drop_table_sql("tbl_account"), "drop table if exists tbl_account;")

	def test_select(self):
		self.assertEqual(
			self.ctrl_base.get_select_sql("tbl_account"),
			(
				"select `account`.`id`, `account`.`password`, `account`.`name`, `account`.`admin`, `auth`.`function` as `auth_function` from `tbl_account` as `account` inner join `tbl_auth` as `auth` on `account`.`id` = `auth`.`id`;",
				[]
			)
		)

	def test_select2(self):
		self.assertEqual(
			self.ctrl_base.get_select_sql("tbl_account", {"id": "test"}, ["password"]),
			(
				"select `account`.`id`, `account`.`name`, `account`.`admin`, `auth`.`function` as `auth_function` from `tbl_account` as `account` inner join `tbl_auth` as `auth` on `account`.`id` = `auth`.`id` where (`account`.`id` = ?);",
				["test"]
			)
		)

	def test_insert(self):
		self.assertEqual(
			self.ctrl_base.get_insert_sql("tbl_account", [{"id": "test1", "password": "hoge", "name": "Pon!"}, {"id": "test1", "password": "hoo"}]),
			[
				("insert into tbl_account (`id`, `password`, `name`, `admin`) values (?, ?, ?, ?);", ["test1", "hoge", "Pon!", True]),
				("insert into tbl_account (`id`, `password`, `name`, `admin`) values (?, ?, ?, ?);", ["test1", "hoo", None, True])
			]
		)

	def test_insert_upsert_updates_non_key_columns(self):
		"""UPSERTの更新対象はキー列ではなく更新可能な列（role="up"）"""
		sql, params = self.ctrl_base.get_insert_sql(
			"tbl_account", [{"id": "test1", "password": "hoge", "name": "Pon!"}], True)[0]
		self.assertIn(
			" on duplicate key update `password` = ?, `name` = ?, `admin` = ?;", sql)
		# INSERTの値に続けて、更新対象列の値がバインドされる
		self.assertEqual(params, ["test1", "hoge", "Pon!", True, "hoge", "Pon!", True])
		# キー列（id）は更新対象に含めない（同じ値で上書きするだけになるため）
		self.assertNotIn("`id` = ?;", sql)

	def test_update(self):
		self.assertEqual(
			self.ctrl_base.get_update_sql("tbl_account", {"admin": "false"}, { "name": "test" }),
			("update `tbl_account` set `admin` = ? where `name` = ?;", [False, "test"])
		)

	def test_increment(self):
		"""加算はDB側の式で行い、加算値のみをバインドする"""
		self.assertEqual(
			self.ctrl_base.get_increment_sql("tbl_account", {"admin": 1}, {"id": "test1"}),
			("update `tbl_account` set `admin` = coalesce(`admin`, 0) + ? where `id` = ?;",
			 [1, "test1"])
		)

	def test_increment_without_target_column_returns_none(self):
		"""加算対象の列がない場合は(None, [])を返す"""
		self.assertEqual(
			self.ctrl_base.get_increment_sql("tbl_account", {}, {"id": "test1"}),
			(None, [])
		)

	def test_increment_unknown_table_returns_none(self):
		"""未定義テーブルは(None, [])を返す"""
		self.assertEqual(self.ctrl_base.get_increment_sql("tbl_unknown", {"admin": 1}), (None, []))

	def test_delete(self):
		self.assertEqual(
			self.ctrl_base.get_delete_sql("tbl_account", [{"id": "test1"}, {"id": "test2"}]),
			[
				("delete from `tbl_account` where `id` = ?;", ["test1"]),
				("delete from `tbl_account` where `id` = ?;", ["test2"])
			]
		)

	# ---- SQLインジェクション対策（値のバインド） ----

	def test_select_injection_value_is_bound(self):
		"""WHERE句の値に注入文字列を渡してもSQL文は変わらずバインド値になる"""
		injection = "test' OR '1'='1"
		sql, params = self.ctrl_base.get_select_sql("tbl_account", {"id": injection}, ["password"])
		# プレースホルダは1つだけで、注入文字列はSQL文へ現れない
		self.assertEqual(sql.count("?"), 1)
		self.assertNotIn("OR", sql)
		self.assertNotIn("'", sql)
		self.assertEqual(params, [injection])

	def test_select_quote_in_value_is_bound(self):
		"""アポストロフィを含む正当な値もバインド値として扱う（構文エラーにならない）"""
		sql, params = self.ctrl_base.get_select_sql("tbl_account", {"name": "O'Brien"})
		self.assertIn("`account`.`name` = ?", sql)
		self.assertEqual(params, ["O'Brien"])

	def test_select_or_group_params_follow_sql_order(self):
		"""OR条件（配列指定）のバインド値がSQL文の並びと一致する"""
		sql, params = self.ctrl_base.get_select_sql(
			"tbl_account", [{"id": "a"}, {"name": "b"}], ["password"])
		self.assertIn("where (`account`.`id` = ?) or (`account`.`name` = ?);", sql)
		self.assertEqual(params, ["a", "b"])

	def test_select_in_condition_binds_each_value(self):
		"""IN句は値ごとにプレースホルダを生成する"""
		sql, params = self.ctrl_base.get_select_sql("tbl_account", {"id": ["a", "b", "c"]})
		self.assertIn("`account`.`id` in (?,?,?)", sql)
		self.assertEqual(params, ["a", "b", "c"])

	def test_select_like_condition_binds_value(self):
		"""LIKEの値もバインドする（従来は素で埋め込んでいた経路）"""
		sql, params = self.ctrl_base.get_select_sql(
			"tbl_account", {"name": {"value": "%a'b%", "operator": "like"}})
		self.assertIn("`account`.`name` like ?", sql)
		self.assertEqual(params, ["%a'b%"])

	def test_select_between_condition_binds_two_values(self):
		"""BETWEENは2値をバインドする"""
		sql, params = self.ctrl_base.get_select_sql(
			"tbl_account", {"id": {"value": ["a", "b"], "operator": "between"}})
		self.assertIn("`account`.`id` between ? and ?", sql)
		self.assertEqual(params, ["a", "b"])

	def test_select_none_value_is_null_check_without_param(self):
		"""None指定はis null（バインド値を持たない）"""
		sql, params = self.ctrl_base.get_select_sql("tbl_account", {"name": None})
		self.assertIn("`account`.`name` is null", sql)
		self.assertEqual(params, [])

	def test_update_params_are_set_then_where(self):
		"""UPDATEのバインド値はSET句→WHERE句の順に並ぶ"""
		sql, params = self.ctrl_base.get_update_sql(
			"tbl_account", {"admin": "false"}, {"name": "x' OR 1=1 --"})
		self.assertEqual(sql, "update `tbl_account` set `admin` = ? where `name` = ?;")
		self.assertEqual(params, [False, "x' OR 1=1 --"])

	def test_update_without_target_column_returns_none(self):
		"""更新対象の列がない場合は(None, [])を返す"""
		self.assertEqual(
			self.ctrl_base.get_update_sql("tbl_account", {}, {"name": "test"}),
			(None, [])
		)

	def test_select_unknown_table_returns_none(self):
		"""未定義テーブルは(None, [])を返す"""
		self.assertEqual(self.ctrl_base.get_select_sql("tbl_unknown"), (None, []))
