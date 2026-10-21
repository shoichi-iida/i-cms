#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from tornado.options import define, options
from functions.define.base_define import BaseDefine
from functions.common.migrate_data import MigrateData
from functions.common.util_encrypt import UtilEncrypt
from functions.data.control_sqlite import ControlSqlite
from functions.data.control_tinydb import ControlTinyDB

# bcryptのコストファクタを最小にしてテストの実行時間を抑える
if "bcrypt_rounds" not in options:
	define("bcrypt_rounds", default=12, type=int)
options.bcrypt_rounds = UtilEncrypt.BCRYPT_MIN_ROUNDS

ENCRYPT_KEY = "i-cms"
PASSWORD = "P@ssw0rd"

# 権限定義（define/common/auth.xml相当）
CTRL_DEFINE = {
	"auth": {
		"def": {
			"ref": {"id": "ref", "name": "参照権限", "ref_name": "参照"},
			"up": {"id": "up", "name": "更新権限", "ref_name": "更新"},
		}
	}
}

def load_tables(ctrl):
	for tbl in BaseDefine("define/tables/db_control.xml").dict.values():
		ctrl.tables[tbl["id"]] = tbl

class TestMigrateDataSqlite(TestCase):
	"""旧定義（auth_id・XOR暗号）で作成したSQLiteのDBの移行"""

	def setUp(self):
		self.ctrl = ControlSqlite()
		load_tables(self.ctrl)
		# 旧定義のテーブル
		self.ctrl.exec_sql("create table mst_auth (id text, name text, function text, operation integer, primary key(id))")
		self.ctrl.exec_sql("create table tbl_account (id integer, user_id text, password text, name text, admin boolean, is_active boolean, primary key(id))")
		self.ctrl.exec_sql("create table tbl_auth (id integer, auth_id text, auth_value boolean, primary key(id, auth_id))")
		self.ctrl.exec_sql("insert into mst_auth values ('ref', '参照権限', '参照', null), ('up', '更新権限', '更新', null)")
		self.ctrl.exec_sql("insert into tbl_account values (0, 'admin', ?, '管理者', 1, 1), (1, 'nopass', '', '未設定', 0, 1)",
						   (UtilEncrypt.encrypt_xor(PASSWORD, ENCRYPT_KEY),))
		self.ctrl.exec_sql("insert into tbl_auth values (0, 'ref', 1), (0, 'up', 0)")
		self.logs = []

	def migrate(self, encrypt_key=ENCRYPT_KEY):
		return MigrateData({"db_control": self.ctrl}, CTRL_DEFINE, encrypt_key, self.logs.append).exec()

	def get_tables(self):
		return set(r["name"] for r in self.ctrl.exec_sql("select name from sqlite_master where type = 'table'"))

	def test_migrate(self):
		"""移行: 列の追加・権限キーの変換・追加列の補完・パスワードのbcrypt化を行う"""
		self.assertEqual(self.migrate(), [])

		accounts = {r["user_id"]: r for r in self.ctrl.select("tbl_account")}
		admin = accounts["admin"]
		self.assertTrue(UtilEncrypt.is_hashed_password(admin["password"]))
		self.assertTrue(UtilEncrypt.verify_password(PASSWORD, admin["password"]))
		self.assertEqual(admin["is_password_set"], 1)
		self.assertEqual(admin["login_failed_count"], 0)
		self.assertIsNone(admin["login_locked_until"])
		self.assertIsNotNone(admin["created_at"])
		self.assertEqual(admin["created_by"], 0)
		# パスワード未設定のアカウントはパスワードを変えず、未設定として扱う
		self.assertEqual(accounts["nopass"]["password"], "")
		self.assertEqual(accounts["nopass"]["is_password_set"], 0)

		# 権限キーがmst_auth.functionへ変換され、mst_authと結合できる
		auth = {r["function"]: r for r in self.ctrl.select("tbl_auth", {"id": 0})}
		self.assertEqual(set(auth.keys()), {"参照", "更新"})
		self.assertEqual(auth["参照"]["auth_value"], 1)
		self.assertEqual(auth["参照"]["auth_name"], "参照権限")

		# 変更前のテーブルをバックアップしている
		backups = [t for t in self.get_tables() if "_bak_" in t]
		self.assertEqual(sorted(t.split("_bak_")[0] for t in backups), ["tbl_account", "tbl_auth"])
		bak_auth = [t for t in backups if t.startswith("tbl_auth_bak_")][0]
		self.assertEqual(len(self.ctrl.exec_sql("select auth_id from {0}".format(bak_auth))), 2)

		# 定義にあり、DBに存在しないテーブルを作成している
		self.assertIn("tbl_account_token", self.get_tables())

	def test_migrate_is_idempotent(self):
		"""再実行: 移行済みのデータ・ハッシュは変更せず、バックアップも取らない"""
		self.migrate()
		before = self.ctrl.select("tbl_account")
		tables = self.get_tables()
		self.logs.clear()
		self.assertEqual(self.migrate(), [])
		self.assertEqual(self.ctrl.select("tbl_account"), before)
		self.assertEqual(self.get_tables(), tables)
		self.assertFalse(any("バックアップ" in m for m in self.logs))

	def test_migrate_reports_undecryptable_password(self):
		"""異常系: 復号できないパスワードは変更せず、user_idを返す"""
		self.ctrl.exec_sql("update tbl_account set password = 'not-hex' where id = 0")
		self.assertEqual(self.migrate(), ["admin"])
		admin = self.ctrl.select("tbl_account", {"id": 0})[0]
		self.assertEqual(admin["password"], "not-hex")

class TestMigrateDataTinyDB(TestCase):
	"""旧定義（auth_id・XOR暗号）で作成したTinyDBのDBの移行"""

	def setUp(self):
		self.ctrl = ControlTinyDB(None)
		load_tables(self.ctrl)
		self.ctrl.db.table("mst_auth").insert_multiple([
			{"id": "ref", "name": "参照権限", "function": "参照", "operation": None},
			{"id": "up", "name": "更新権限", "function": "更新", "operation": None},
		])
		self.ctrl.db.table("tbl_account").insert({
			"id": 0, "user_id": "admin", "password": UtilEncrypt.encrypt_xor(PASSWORD, ENCRYPT_KEY),
			"name": "管理者", "admin": True, "is_active": True})
		self.ctrl.db.table("tbl_auth").insert_multiple([
			{"id": 0, "auth_id": "ref", "auth_value": True},
			{"id": 0, "auth_id": "up", "auth_value": False},
		])

	def migrate(self):
		return MigrateData({"db_control": self.ctrl}, CTRL_DEFINE, ENCRYPT_KEY, lambda m: None).exec()

	def test_migrate(self):
		"""移行: 権限キーの変換・追加列の補完・パスワードのbcrypt化を行い、再実行しても変わらない"""
		self.assertEqual(self.migrate(), [])
		admin = self.ctrl.db.table("tbl_account").all()[0]
		self.assertTrue(UtilEncrypt.verify_password(PASSWORD, admin["password"]))
		self.assertTrue(admin["is_password_set"])
		self.assertEqual(admin["login_failed_count"], 0)
		auth = self.ctrl.db.table("tbl_auth").all()
		self.assertEqual(sorted(r["function"] for r in auth), ["参照", "更新"])
		self.assertFalse(any("auth_id" in r for r in auth))

		hashed = admin["password"]
		self.assertEqual(self.migrate(), [])
		self.assertEqual(self.ctrl.db.table("tbl_account").all()[0]["password"], hashed)
