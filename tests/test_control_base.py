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
	def delete(self, tbl_id, lst_delete=[]): pass
	def escape(self, col_type, val):return super().escape(col_type, val)

class TestControlBase(TestCase):
	def setUp(self):
		self.ctrl_base = MockControl()
		def_tbl = BaseDefine("tests/data/test_tables.xml").dict
		for tbl in def_tbl.values():
			self.ctrl_base.tables[tbl["id"]] = tbl

	def test_create_table(self):
		self.assertEqual(self.ctrl_base.get_create_table_sql("tbl_account"), "create table if not exists tbl_account (id varchar, password varchar, name varchar, admin boolean, primary key(id));")

	def test_drop_table(self):
		self.assertEqual(self.ctrl_base.get_drop_table_sql("tbl_account"), "drop table if exists tbl_account;")

	def test_select(self):
		self.assertEqual(
			self.ctrl_base.get_select_sql("tbl_account"),
			"select `account`.`id`, `account`.`password`, `account`.`name`, `account`.`admin`, `auth`.`function` as `auth_function` from `tbl_account` as `account` inner join `tbl_auth` as `auth` on `account`.`id` = `auth`.`id`;"
		)

	def test_select2(self):
		self.assertEqual(
			self.ctrl_base.get_select_sql("tbl_account", {"id": "test"}, ["password"]),
			"select `account`.`id`, `account`.`name`, `account`.`admin`, `auth`.`function` as `auth_function` from `tbl_account` as `account` inner join `tbl_auth` as `auth` on `account`.`id` = `auth`.`id` where `account`.`id` = 'test';"
		)

	def test_insert(self):
		self.assertEqual(
			self.ctrl_base.get_insert_sql("tbl_account", [{"id": "test1", "password": "hoge", "name": "Pon!"}, {"id": "test1", "password": "hoo"}]),
			[
				"insert into tbl_account (`id`, `password`, `name`, `admin`) values ('test1', 'hoge', 'Pon!', true);",
				"insert into tbl_account (`id`, `password`, `name`, `admin`) values ('test1', 'hoo', null, true);"
			]
		)

	def test_update(self):
		self.assertEqual(
			self.ctrl_base.get_update_sql("tbl_account", {"admin": "false"}, { "name": "test" }),
			"update `tbl_account` set `admin` = false where `name` = 'test';"
		)

	def test_delete(self):
		self.assertEqual(
			self.ctrl_base.get_delete_sql("tbl_account", [{"id": "test1"}, {"id": "test2"}]),
			"delete from `tbl_account` where `id` = 'test1';delete from `tbl_account` where `id` = 'test2';"
		)
