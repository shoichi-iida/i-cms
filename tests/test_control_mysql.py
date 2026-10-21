#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
MySQL制御クラス（ControlMysql）の単体テスト

対応仕様書: docs/単体テスト仕様書/DBアクセス層（SQL生成・パラメータバインド）.md

開発・テスト環境はSQLiteのため実MySQLへは接続せず、pymysql.connectを
差し替えた偽コネクションでcursor.executeへ渡る内容を検証する。
検証対象は「MySQL経路でも値がSQL文へ埋め込まれずバインドされること」と、
プレースホルダ記法（%s）・%書式の扱いがSQLiteと異なる点である。
"""

import os
import sys
from unittest import TestCase

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pymysql

from functions.define.base_define import BaseDefine
from functions.data.control_mysql import ControlMysql


class FakeCursor:
    """cursor.executeの呼び出し内容を記録する偽カーソル"""

    def __init__(self, calls, lastrowid=None):
        self.calls = calls
        self.lastrowid = lastrowid

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql, *args):
        # argsを渡さずに呼ばれた場合と区別できるようタプルのまま記録する
        self.calls.append((sql, args))

    def fetchall(self):
        return []


class FakeConnection:
    def __init__(self):
        self.calls = []
        # INSERT後のcursor.lastrowid（AUTO_INCREMENTで採番されたID）として返す値
        self.lastrowid = None

    def cursor(self):
        return FakeCursor(self.calls, self.lastrowid)

    def begin(self):
        self.calls.append(("BEGIN", ()))

    def close(self):
        pass


class TestControlMysql(TestCase):
    def setUp(self):
        self.fake_db = FakeConnection()
        self.original_connect = pymysql.connect
        pymysql.connect = lambda **kwargs: self.fake_db
        self.ctrl = ControlMysql("host", 3306, "user", "password", "db", "utf8mb4")
        def_tbl = BaseDefine("tests/data/test_tables.xml").dict
        for tbl in def_tbl.values():
            self.ctrl.tables[tbl["id"]] = tbl

    def tearDown(self):
        pymysql.connect = self.original_connect

    def last_call(self):
        return self.fake_db.calls[-1]

    def test_placeholder_is_mysql_style(self):
        """プレースホルダは%s（SQLiteの?と異なる）"""
        self.assertEqual(self.ctrl.placeholder, "%s")

    def test_create_table_autoincrement_uses_auto_increment(self):
        """採番列（autoincrement="true"）はMySQLのAUTO_INCREMENTになる（SQLiteのautoincrementと異なる）"""
        self.ctrl.create_table("tbl_sequence")
        sql, args = self.last_call()
        self.assertEqual(
            sql,
            "create table if not exists tbl_sequence (id integer primary key auto_increment, name text);")
        self.assertEqual(args, ())

    def test_create_table_text_key_uses_varchar(self):
        """主キーのtext列はvarchar(255)になる（MySQLはTEXT型の列を長さ指定なしで主キーにできない）"""
        self.ctrl.create_table("tbl_auth")
        sql, args = self.last_call()
        self.assertEqual(
            sql,
            "create table if not exists tbl_auth (id varchar(255), function text, value boolean, primary key(id));")
        self.assertEqual(args, ())

    def test_insert_returns_generated_id(self):
        """idを指定しないINSERTは、AUTO_INCREMENTで採番したid（cursor.lastrowid）を[{"id": ...}]で返す

        SQLite（returning句）と同じ形で返し、IAE002が[0]["id"]で採番したidを参照できるようにする。
        """
        self.fake_db.lastrowid = 7
        result = self.ctrl.insert("tbl_sequence", [{"name": "a"}])
        self.assertEqual(result, [{"id": 7}])
        sql, args = self.last_call()
        self.assertIn("values (%s, %s)", sql)
        self.assertEqual(args, ([None, "a"],))

    def test_insert_returns_specified_id(self):
        """idを指定したINSERTは、指定したidをレコードごとに返す（cursor.lastrowidは使わない）"""
        self.fake_db.lastrowid = 99
        result = self.ctrl.insert("tbl_sequence", [{"id": 3, "name": "a"}, {"id": 4, "name": "b"}])
        self.assertEqual(result, [{"id": 3}, {"id": 4}])
        self.assertEqual(len(self.fake_db.calls), 2)

    def test_insert_zero_into_autoincrement_returns_generated_id(self):
        """AUTO_INCREMENTの列へ0を指定したINSERTは、MySQLが採番したid（cursor.lastrowid）を返す"""
        self.fake_db.lastrowid = 8
        self.assertEqual(self.ctrl.insert("tbl_sequence", [{"id": 0, "name": "a"}]), [{"id": 8}])
        # 整数の文字列（"0"）も0として扱う
        self.assertEqual(self.ctrl.insert("tbl_sequence", [{"id": "0", "name": "a"}]), [{"id": 8}])
        # NO_AUTO_VALUE_ON_ZEROモードで0がそのまま保存された場合はlastrowidも0
        self.fake_db.lastrowid = 0
        self.assertEqual(self.ctrl.insert("tbl_sequence", [{"id": 0, "name": "a"}]), [{"id": 0}])

    def test_begin_rejects_serialize_inside_non_serialized_transaction(self):
        """直列化していないトランザクションの中での直列化は、書き込みロックを取らずに例外にする

        REPEATABLE READでは最初の読み取り時点のスナップショットを読み続けるため、
        途中でロックを取っても、それまでにコミットされた変更を見落とすため。
        """
        self.ctrl.begin()
        with self.assertRaises(Exception):
            self.ctrl.begin(serialize=True)
        self.assertFalse(any("get_lock" in sql for sql, args in self.fake_db.calls))
        self.assertFalse(self.ctrl.write_lock_held)

    def test_insert_zero_into_non_autoincrement_returns_zero(self):
        """採番列でないid列へ0を指定したINSERTは、指定した0を返す"""
        self.fake_db.lastrowid = 8
        self.assertEqual(self.ctrl.insert("tbl_auth", [{"id": 0, "function": "f"}]), [{"id": 0}])

    def test_insert_without_id_column_returns_empty(self):
        """id列を持たないテーブルのINSERTは、従来どおりfetchallの結果（空配列）を返す"""
        self.fake_db.lastrowid = 7
        self.assertEqual(self.ctrl.insert("tbl_setting", [{"key": "k", "value": "v"}]), [])

    def test_select_binds_value_with_mysql_placeholder(self):
        """SELECTの条件値はSQL文へ埋め込まれず%sでバインドされる"""
        self.ctrl.select("tbl_account", {"id": "test' OR '1'='1"}, ["password"])
        sql, args = self.last_call()
        self.assertIn("`account`.`id` = %s", sql)
        self.assertNotIn("'", sql)
        self.assertEqual(args, (["test' OR '1'='1"],))

    def test_insert_binds_values(self):
        """INSERTの値も%sでバインドされる"""
        self.ctrl.insert("tbl_account", [{"id": "test1", "password": "hoge", "name": "Pon!"}])
        sql, args = self.last_call()
        self.assertIn("values (%s, %s, %s, %s)", sql)
        self.assertEqual(args, (["test1", "hoge", "Pon!", True],))

    def test_insert_upsert_updates_non_key_columns(self):
        """UPSERT（on duplicate key update）は更新可能な列を更新する"""
        self.ctrl.insert(
            "tbl_account", [{"id": "test1", "password": "hoge", "name": "Pon!"}], True)
        sql, args = self.last_call()
        self.assertIn(
            " on duplicate key update `password` = %s, `name` = %s, `admin` = %s;", sql)
        self.assertEqual(args, (["test1", "hoge", "Pon!", True, "hoge", "Pon!", True],))

    def test_update_binds_set_then_where(self):
        """UPDATEはSET句→WHERE句の順にバインドされる"""
        self.ctrl.update("tbl_account", {"admin": "false"}, {"name": "test"})
        sql, args = self.last_call()
        self.assertEqual(sql, "update `tbl_account` set `admin` = %s where `name` = %s;")
        self.assertEqual(args, ([False, "test"],))

    def test_increment_binds_only_the_added_value(self):
        """加算はDB側の式で行い、加算値とWHERE値のみをバインドする"""
        self.ctrl.increment("tbl_account", {"admin": 1}, {"id": "test1"})
        sql, args = self.last_call()
        self.assertEqual(
            sql,
            "update `tbl_account` set `admin` = coalesce(`admin`, 0) + %s"
            " where `id` = %s;")
        self.assertEqual(args, ([1, "test1"],))

    def test_increment_without_target_column_does_not_execute(self):
        """加算対象の列がない場合はSQLを実行しない"""
        self.assertEqual(self.ctrl.increment("tbl_account", {}, {"id": "test1"}), [])
        self.assertEqual(len(self.fake_db.calls), 0)

    def test_delete_executes_once_per_record(self):
        """DELETEは条件ごとに1文ずつ実行される（複数文の連結ではない）"""
        self.ctrl.delete("tbl_account", [{"id": "test1"}, {"id": "test2"}])
        self.assertEqual(len(self.fake_db.calls), 2)
        for (sql, args), expected in zip(self.fake_db.calls, (["test1"], ["test2"])):
            self.assertEqual(sql, "delete from `tbl_account` where `id` = %s;")
            self.assertEqual(args, (expected,))

    def test_exec_sql_without_params_omits_args(self):
        """バインド値がない場合はargsを渡さない（リテラルの%を壊さないため）"""
        self.ctrl.exec_sql("select '100%' as pct from tbl_account;")
        sql, args = self.last_call()
        self.assertEqual(sql, "select '100%' as pct from tbl_account;")
        self.assertEqual(args, ())

    def test_exec_sql_with_params_passes_args(self):
        """バインド値がある場合はargsを渡す（pymysqlが%s書式を適用する）"""
        self.ctrl.exec_sql("select * from tbl_account where id = %s;", ["test1"])
        sql, args = self.last_call()
        self.assertEqual(args, (["test1"],))

    def test_exec_sql_none_returns_empty(self):
        """SQLがNone（更新対象なし等）の場合は実行せず空配列を返す"""
        self.assertEqual(self.ctrl.exec_sql(None), [])
        self.assertEqual(len(self.fake_db.calls), 0)

    def test_update_without_target_column_does_not_execute(self):
        """更新対象の列がない場合はSQLを実行しない"""
        self.assertEqual(self.ctrl.update("tbl_account", {}, {"name": "test"}), [])
        self.assertEqual(len(self.fake_db.calls), 0)
