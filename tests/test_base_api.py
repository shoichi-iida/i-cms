#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from functions.api.base_api import BaseApi
from functions.define.base_define import BaseDefine
from functions.data.control_tinydb import ControlTinyDB

class FakeHandler:
	def __init__(self, ctrl):
		self.ctrl_db = {"db_control": ctrl}

class TestResetLoginFailure(TestCase):
	"""ログイン失敗回数・ロックのクリア（reset_login_failure）"""

	def setUp(self):
		self.ctrl = ControlTinyDB(None)
		for tbl in BaseDefine("define/tables/db_control.xml").dict.values():
			self.ctrl.tables[tbl["id"]] = tbl
		# 初期化（InitializeData）と同じく数値のIDで保存する
		self.ctrl.insert("tbl_account", [
			{"id": 0, "user_id": "admin", "login_failed_count": 5, "login_locked_until": "2099-01-01T00:00:00"},
			{"id": 1, "user_id": "user", "login_failed_count": 3, "login_locked_until": "2099-01-01T00:00:00"},
		])

	def account(self, account_id):
		return [r for r in self.ctrl.db.table("tbl_account").all() if r["id"] == account_id][0]

	def test_reset_with_string_and_int_ids(self):
		"""URLの文字列のIDとDBの数値のIDのどちらでも、TinyDBの行をクリアできる"""
		BaseApi().reset_login_failure(FakeHandler(self.ctrl), "0", 1)
		for account_id in (0, 1):
			self.assertEqual(self.account(account_id)["login_failed_count"], 0)
			self.assertIsNone(self.account(account_id)["login_locked_until"])

	def test_normalize_account_id(self):
		"""数値として解釈できるIDは数値に、それ以外はそのまま返す"""
		api = BaseApi()
		self.assertEqual(api.normalize_account_id("0"), 0)
		self.assertEqual(api.normalize_account_id(12), 12)
		self.assertEqual(api.normalize_account_id("abc"), "abc")
		self.assertIsNone(api.normalize_account_id(None))

class TestIsAdminAccount(TestCase):
	"""管理者判定（is_admin_account）。TinyDBは保存した文字列をそのまま返す"""

	def is_admin(self, value):
		handler = FakeHandler(None)
		handler.prm_cmn = {"account_data": {"admin": value}}
		return BaseApi().is_admin_account(handler)

	def test_true_values(self):
		for value in (True, 1, "1", "true", "True", " TRUE "):
			self.assertTrue(self.is_admin(value), value)

	def test_false_values(self):
		"""空でない文字列でも"0"・"false"は管理者として扱わない"""
		for value in (False, 0, "0", "false", "False", "", None, "no"):
			self.assertFalse(self.is_admin(value), value)

	def test_without_account_data(self):
		handler = FakeHandler(None)
		handler.prm_cmn = {}
		self.assertFalse(BaseApi().is_admin_account(handler))
