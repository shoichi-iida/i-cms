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
from functions.data.control_mysql import ControlMysql
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

	def test_migrate_auth_keeps_original_table_on_failure(self):
		"""異常系: 作業用テーブルへの投入に失敗した場合、元のtbl_authは変更されず作業用テーブルも残らない"""
		def fail(*args, **kwargs):
			raise RuntimeError("insert failed")
		self.ctrl.insert = fail
		with self.assertRaises(RuntimeError):
			self.migrate()
		rows = self.ctrl.exec_sql("select auth_id from tbl_auth order by auth_id")
		self.assertEqual([r["auth_id"] for r in rows], ["ref", "up"])
		self.assertFalse(any("_mig_" in t for t in self.get_tables()))
		self.assertFalse(any(t.startswith("tbl_auth_mig_") for t in self.ctrl.tables))

class TestMigrateDataLegacySetupSql(TestCase):
	"""旧setup.sqlのレイアウト（mst_auth(auth_id, auth_name, ...)・tbl_auth.function）で作成したDBの移行"""

	def setUp(self):
		self.ctrl = ControlSqlite()
		load_tables(self.ctrl)
		self.ctrl.exec_sql("pragma foreign_keys = on")
		self.ctrl.exec_sql("create table mst_auth (auth_id varchar(36) not null, auth_name varchar(30) not null, function varchar(50) not null, operation integer, primary key(auth_id), unique(function))")
		self.ctrl.exec_sql("create table mst_menu_auth (menu_id varchar(36) not null, auth_id varchar(36) not null, primary key(menu_id, auth_id), foreign key(auth_id) references mst_auth(auth_id))")
		self.ctrl.exec_sql("create table tbl_account (id integer, user_id varchar(30) not null, password varchar(30) not null, name varchar(30) not null, admin boolean, is_active boolean, primary key(id))")
		self.ctrl.exec_sql("create table tbl_auth (id varchar(30) not null, function varchar(50) not null, auth_value boolean, primary key(id, function), foreign key(function) references mst_auth(function))")
		self.ctrl.exec_sql("insert into mst_auth values ('a1', '参照権限', '参照', 0)")
		self.ctrl.exec_sql("insert into mst_menu_auth values ('m1', 'a1')")
		self.ctrl.exec_sql("insert into tbl_account values (0, 'admin', ?, '管理者', 1, 1)",
						   (UtilEncrypt.encrypt_xor(PASSWORD, ENCRYPT_KEY),))
		self.ctrl.exec_sql("insert into tbl_auth values ('0', '参照', 1)")
		self.ctrl.exec_sql("create table tbl_account_token (id integer not null, token varchar(50) not null, expires_at date not null, revoked boolean, primary key(id, token), foreign key(id) references tbl_account(id))")
		self.ctrl.exec_sql("insert into tbl_account_token values (0, 'old-token', '2026-10-21', 0)")

	def test_migrate(self):
		"""移行: mst_authの列名を変更し（値と外部キーの参照を保つ）、パスワード等の移行まで行う"""
		self.assertEqual(
			MigrateData({"db_control": self.ctrl}, CTRL_DEFINE, ENCRYPT_KEY, lambda m: None).exec(), [])
		self.assertEqual(self.ctrl.select("mst_auth"),
						 [{"id": "a1", "name": "参照権限", "function": "参照", "operation": 0}])
		# ほかのテーブルからの外部キーの参照も新しい列名へ付け替わる
		menu_auth_sql = self.ctrl.exec_sql(
			"select sql from sqlite_master where name = 'mst_menu_auth'")[0]["sql"]
		self.assertIn("references mst_auth(id)", menu_auth_sql.replace('"', ""))
		self.assertEqual(self.ctrl.exec_sql("pragma foreign_key_check"), [])
		# tbl_authは既にfunctionのため変換しない
		self.assertEqual([r["auth_name"] for r in self.ctrl.select("tbl_auth")], ["参照権限"])
		admin = self.ctrl.select("tbl_account")[0]
		self.assertTrue(UtilEncrypt.verify_password(PASSWORD, admin["password"]))

		# tbl_account_tokenは現在の定義（主キーid・text型）で作り直し、既存のセッションは破棄する
		columns = {r["name"]: r for r in self.ctrl.exec_sql("pragma table_info(tbl_account_token)")}
		self.assertEqual(set(n for n, r in columns.items() if r["pk"] > 0), {"id"})
		self.assertEqual(columns["token"]["type"].lower(), "text")
		self.assertEqual(columns["expires_at"]["type"].lower(), "text")
		self.assertEqual(self.ctrl.select("tbl_account_token"), [])
		bak = [r["name"] for r in self.ctrl.exec_sql(
			"select name from sqlite_master where name like 'tbl_account_token_bak_%'")]
		self.assertEqual(len(bak), 1)
		self.assertEqual(self.ctrl.exec_sql("select token from {0}".format(bak[0])), [{"token": "old-token"}])
		self.assertEqual(self.ctrl.exec_sql(
			"select name from sqlite_master where name like 'tbl_account_token_%' and name not like '%_bak_%'"), [])
		# 作り直したテーブルでログイン時のUPSERTが前のトークンを置き換える
		self.ctrl.insert("tbl_account_token", [{"id": "0", "token": "t1", "expires_at": "x", "revoked": False}], True)
		self.ctrl.insert("tbl_account_token", [{"id": "0", "token": "t2", "expires_at": "x", "revoked": False}], True)
		self.assertEqual([r["token"] for r in self.ctrl.select("tbl_account_token")], ["t2"])

	def test_migrate_current_token_table_is_kept(self):
		"""現在の定義のtbl_account_tokenは作り直さず、既存のセッションを残す"""
		self.ctrl.exec_sql("drop table tbl_account_token")
		self.ctrl.create_table("tbl_account_token")
		self.ctrl.insert("tbl_account_token", [{"id": "0", "token": "t1", "expires_at": "x", "revoked": False}])
		MigrateData({"db_control": self.ctrl}, CTRL_DEFINE, ENCRYPT_KEY, lambda m: None).exec()
		self.assertEqual([r["token"] for r in self.ctrl.select("tbl_account_token")], ["t1"])

class FakeMysqlCtrl(ControlMysql):
	"""information_schemaの問い合わせに決まった結果を返し、実行したSQLを記録するMySQL制御クラス"""

	def __init__(self, password_column):
		self.tables = {}
		self.placeholder = "%s"
		self.password_column = password_column
		self.executed = []

	def __del__(self):
		pass

	def exec_sql(self, sql, param=()):
		self.executed.append(sql)
		if "information_schema" in sql:
			return [self.password_column] if self.password_column else []
		return []

class TestMigrateDataMysqlPassword(TestCase):
	"""MySQLのtbl_account.passwordの列の拡張"""

	def widen(self, password_column):
		ctrl = FakeMysqlCtrl(password_column)
		load_tables(ctrl)
		MigrateData({"db_control": ctrl}, CTRL_DEFINE, ENCRYPT_KEY, lambda m: None).widen_password_column(ctrl)
		return [sql for sql in ctrl.executed if sql.startswith("alter")]

	def test_widen_short_varchar(self):
		"""旧setup.sqlのVARCHAR(30) NOT NULLは、NOT NULLを保ったままtext型へ広げる"""
		self.assertEqual(
			self.widen({"data_type": "varchar", "length": 30, "is_nullable": "NO"}),
			["alter table tbl_account modify column password text not null"])

	def test_keep_wide_column(self):
		"""十分な長さの文字列型・text型は変更しない"""
		self.assertEqual(self.widen({"data_type": "varchar", "length": 255, "is_nullable": "YES"}), [])
		self.assertEqual(self.widen({"data_type": "text", "length": 65535, "is_nullable": "YES"}), [])
		self.assertEqual(self.widen(None), [])

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
		self.ctrl.db.table("tbl_account_token").insert(
			{"id": 0, "token": "t1", "expires_at": "2099-01-01 00:00:00", "revoked": False})

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
		# トークンのアカウントIDは文字列にそろえる（JWTのsubで延長・無効化できるように）
		self.assertEqual(self.ctrl.db.table("tbl_account_token").all()[0]["id"], "0")

		hashed = admin["password"]
		self.assertEqual(self.migrate(), [])
		self.assertEqual(self.ctrl.db.table("tbl_account").all()[0]["password"], hashed)
