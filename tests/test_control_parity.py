#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
条件の解釈がSQL実装（SQLite）とTinyDB実装で一致することの単体テスト

同じデータ・同じ条件で検索・更新・加算・削除を行い、結果を比べる。
TinyDBが条件を一致比較だけで解釈すると、リスト（IN）・演算子指定の条件で
SQL実装なら対象になる行を黙って飛ばすため、その回帰を防ぐ。
"""

import os
import sys
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from functions.define.base_define import BaseDefine
from functions.data.control_sqlite import ControlSqlite
from functions.data.control_tinydb import ControlTinyDB

ROWS = [
	{"id": "a", "function": "apple", "value": True},
	{"id": "b", "function": "Banana", "value": False},
	{"id": "c", "function": "cherry", "value": None},
	# 値がない（NULL）行
	{"id": "d", "function": None, "value": True},
	# ASCII以外の英字（SQLiteのLIKEは大文字・小文字を区別する）
	{"id": "e", "function": "äpfel", "value": False},
]

# 検証する条件（列 → 条件の値）
CONDITIONS = [
	{"id": "a"},
	{"id": ["a", "c"]},
	{"id": {"value": ["a", "c"], "operator": "not"}},
	{"id": {"value": ["a", "b"], "operator": "between"}},
	{"id": {"value": "a", "operator": "!="}},
	{"id": {"value": "b", "operator": ">="}},
	{"function": {"value": "b%", "operator": "like"}},
	{"function": {"value": "%RR%", "operator": "like"}},
	{"function": {"value": "ÄPF%", "operator": "like"}},
	{"function": {"value": "äPF%", "operator": "like"}},
	{"function": {"value": "%", "operator": "like"}},
	# NULLを含む列・リストの IN・NOT IN（SQLのNULLの扱い）
	{"function": ["apple", "cherry"]},
	{"function": ["apple", None]},
	{"function": {"value": ["apple"], "operator": "not"}},
	{"function": {"value": ["apple", None], "operator": "not"}},
	{"value": {"value": ["true"], "operator": "not"}},
	# bool列へのLIKE（パターンは真偽値へ変換せず、保存値の"1"/"0"と照合する）
	{"value": {"value": "1%", "operator": "like"}},
	{"value": {"value": "0", "operator": "like"}},
	{"value": {"value": "%", "operator": "like"}},
	{"value": {"value": "true", "operator": "like"}},
	{"value": None},
	{"value": "false"},
	{"id": ["a", "b"], "value": True},
]

class TestConditionParity(TestCase):
	def setUp(self):
		self.ctrls = {"sqlite": ControlSqlite(), "tinydb": ControlTinyDB(None)}
		for ctrl in self.ctrls.values():
			for tbl in BaseDefine("tests/data/test_tables.xml").dict.values():
				ctrl.tables[tbl["id"]] = tbl
			ctrl.create_table("tbl_auth")
			ctrl.insert("tbl_auth", ROWS)

	def ids(self, ctrl, dict_select={}):
		return sorted(r["id"] for r in ctrl.select("tbl_auth", dict_select))

	def assert_same(self, label, result):
		self.assertEqual(result["sqlite"], result["tinydb"], label)

	def test_select(self):
		for cond in CONDITIONS:
			self.assert_same(cond, {n: self.ids(c, cond) for n, c in self.ctrls.items()})

	def test_update(self):
		for cond in CONDITIONS:
			self.setUp()
			result = {}
			for name, ctrl in self.ctrls.items():
				ctrl.update("tbl_auth", {"function": "updated"}, cond)
				result[name] = self.ids(ctrl, {"function": "updated"})
			self.assert_same(cond, result)

	def test_increment(self):
		"""加算の対象行も一致する（加算できる整数列がないため、対象行の判定を検索で比べる）"""
		for cond in CONDITIONS:
			self.setUp()
			result = {}
			for name, ctrl in self.ctrls.items():
				before = {r["id"]: r for r in ctrl.select("tbl_auth")}
				# bool列（value）を加算し、変化した行を対象行とする
				ctrl.increment("tbl_auth", {"value": 1}, cond)
				after = {r["id"]: r for r in ctrl.select("tbl_auth")}
				result[name] = sorted(k for k in after if after[k]["value"] != before[k]["value"])
			self.assert_same(cond, result)

	def test_delete(self):
		for cond in CONDITIONS:
			self.setUp()
			result = {}
			for name, ctrl in self.ctrls.items():
				ctrl.delete("tbl_auth", [cond])
				result[name] = self.ids(ctrl)
			self.assert_same(cond, result)

	def test_unsupported_operator_raises(self):
		"""未対応の演算子は黙って不一致にせず、例外にする"""
		with self.assertRaises(ValueError):
			self.ctrls["tinydb"].update("tbl_auth", {"function": "x"}, {"id": {"value": "a", "operator": "regexp"}})
