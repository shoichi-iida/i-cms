#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
ApiHandler.call_api（更新系APIのトランザクションと応答の確定順）の単体テスト

Tornadoのアプリケーション・DBへは接続せず、応答の書き込み・確定とDBの
トランザクション操作を記録する偽物で、呼び出し順を検証する。
"""

import os
import sys
from types import SimpleNamespace
from unittest import TestCase
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from tornado import web
from functions.handler.api_handler import ApiHandler

class FakeDB:
	def __init__(self, events, commit_error=None, rollback_error=None):
		self.events = events
		self.commit_error = commit_error
		self.rollback_error = rollback_error
		self.serialize_writes = False

	def begin(self, serialize=None):
		self.events.append("begin")

	def commit(self):
		self.events.append("commit")
		if self.commit_error is not None:
			raise self.commit_error

	def rollback(self):
		self.events.append("rollback")
		if self.rollback_error is not None:
			raise self.rollback_error

class FakeHandler(ApiHandler):
	"""応答の書き込み・確定・破棄を記録するApiHandler"""

	def __init__(self, db):
		self.ctrl_db = {"db_control": db}
		self.events = db.events
		self.defer_finish = False
		self._finished = False
		self.body = []
		self.status = 200
		# proc_access用
		self.is_error = False
		self.prm_cmn = {}
		self.prm_req = {}
		self.request = SimpleNamespace(path="/api/test")

	def get_operation(self):
		return []

	def get_message(self, msg_cd, msg_prm=[]):
		return msg_cd

	def append_log(self, message, msg_type="normal"):
		pass

	def set_status(self, status_code, reason=None):
		self.status = status_code

	def set_header(self, name, value):
		pass

	def write(self, chunk):
		self.body.append(chunk)

	def finish(self, chunk=None):
		self.events.append("finish")
		self._finished = True

	def clear(self):
		self.events.append("clear")
		self.body = []

class FakeApi:
	def __init__(self, error=None):
		self.error = error
		self.maintenance_use = False
		self.need_login = False
		self.need_auth = ""

	def respond(self, handler, args):
		handler.view_json({"result": "ok"})
		if self.error is not None:
			raise self.error

	get_response = respond
	post_response = respond

class TestCallApi(TestCase):
	def test_finish_after_commit(self):
		"""正常系: 更新系APIの応答は、コミットが成功してから確定する"""
		events = []
		handler = FakeHandler(FakeDB(events))
		handler.call_api(FakeApi(), "post", [])
		self.assertEqual(events, ["begin", "commit", "finish"])
		self.assertEqual(handler.body, ['{"result": "ok"}'])
		self.assertFalse(handler.defer_finish)

	def test_commit_failure_discards_response(self):
		"""異常系: コミットに失敗した場合はロールバックし、書き込んだ応答を確定せずに破棄する"""
		events = []
		handler = FakeHandler(FakeDB(events, RuntimeError("database is locked")))
		with self.assertRaises(RuntimeError):
			handler.call_api(FakeApi(), "post", [])
		self.assertEqual(events, ["begin", "commit", "rollback", "clear"])
		self.assertEqual(handler.body, [])
		self.assertFalse(handler.defer_finish)

	def test_rollback_failure_discards_response(self):
		"""異常系: コミット後のロールバックも失敗した場合（接続断等）も、書き込んだ応答を破棄する"""
		events = []
		handler = FakeHandler(FakeDB(events, RuntimeError("lost connection"), RuntimeError("lost connection")))
		with self.assertRaises(RuntimeError):
			handler.call_api(FakeApi(), "post", [])
		self.assertEqual(events, ["begin", "commit", "rollback", "clear"])
		self.assertEqual(handler.body, [])

	def test_web_finish_rollback_failure_discards_response(self):
		"""異常系: web.Finishの後のロールバックが失敗した場合は、書いた応答を破棄してロールバックの例外を送出する"""
		events = []
		handler = FakeHandler(FakeDB(events, rollback_error=RuntimeError("lost connection")))
		with self.assertRaises(RuntimeError):
			handler.call_api(FakeApi(web.Finish()), "post", [])
		self.assertEqual(events, ["begin", "rollback", "clear"])
		self.assertEqual(handler.body, [])

	def test_api_error_discards_response(self):
		"""異常系: APIが例外を送出した場合はコミットせずロールバックし、応答を破棄する"""
		events = []
		handler = FakeHandler(FakeDB(events))
		with self.assertRaises(ValueError):
			handler.call_api(FakeApi(ValueError("invalid")), "post", [])
		self.assertEqual(events, ["begin", "rollback", "clear"])

	def test_web_finish_keeps_response(self):
		"""正常系: APIがweb.Finishで打ち切った場合はロールバックし、書いた応答は残す（Tornadoが確定する）"""
		events = []
		handler = FakeHandler(FakeDB(events))
		with self.assertRaises(web.Finish):
			handler.call_api(FakeApi(web.Finish()), "post", [])
		self.assertEqual(events, ["begin", "rollback"])
		self.assertEqual(handler.body, ['{"result": "ok"}'])

	def test_get_finishes_immediately(self):
		"""正常系: 読み取り（GET）はトランザクションを使わず、そのまま応答を確定する"""
		events = []
		handler = FakeHandler(FakeDB(events))
		handler.call_api(FakeApi(), "get", [])
		self.assertEqual(events, ["finish"])

class TestProcAccess(TestCase):
	def test_web_finish_propagates(self):
		"""正常系: 更新系APIがweb.Finishで打ち切った場合、proc_accessも500の応答を追記せずに送出する"""
		events = []
		handler = FakeHandler(FakeDB(events))
		handler.pages = [FakeApi(web.Finish())]
		with self.assertRaises(web.Finish):
			handler.proc_access("post", [])
		self.assertEqual(handler.status, 200)
		self.assertEqual(handler.body, ['{"result": "ok"}'])
		self.assertEqual(events, ["begin", "rollback"])

	def test_api_error_returns_500(self):
		"""異常系: APIの例外は書き込んだ応答を破棄し、500のエラー応答だけを確定する"""
		events = []
		handler = FakeHandler(FakeDB(events))
		handler.pages = [FakeApi(ValueError("invalid"))]
		handler.proc_access("post", [])
		self.assertEqual(handler.status, 500)
		self.assertEqual(len(handler.body), 1)
		self.assertIn("CE-9999", handler.body[0])
		self.assertEqual(events, ["begin", "rollback", "clear", "finish"])

class TestTokenAccountId(TestCase):
	"""トークンのアカウントIDの型（TinyDBは条件の型まで一致する必要がある）"""

	def setUp(self):
		from functions.define.base_define import BaseDefine
		from functions.data.control_tinydb import ControlTinyDB
		self.ctrl = ControlTinyDB(None)
		for tbl in BaseDefine("define/tables/db_control.xml").dict.values():
			self.ctrl.tables[tbl["id"]] = tbl
		self.handler = ApiHandler.__new__(ApiHandler)
		self.handler.ctrl_db = {"db_control": self.ctrl}

	def token_row(self):
		return self.ctrl.db.table("tbl_account_token").all()[0]

	def test_extend_and_invalidate_with_jwt_subject(self):
		"""発行時の数値のIDと、JWTのsub（文字列）のIDで、延長・無効化の対象の行が一致する"""
		self.handler.refresh_token(0, "t1")
		self.assertEqual(self.token_row()["id"], "0")
		self.ctrl.db.table("tbl_account_token").update({"expires_at": "2000-01-01 00:00:00"})
		self.handler.extend_token("0", "t1")
		self.assertNotEqual(self.token_row()["expires_at"], "2000-01-01 00:00:00")
		self.handler.invalidate_token("0")
		self.assertTrue(self.token_row()["revoked"])

class TestCheckLogin(TestCase):
	"""ログイン確認（check_login）の一連の流れ（TinyDB。初期化と同じく数値のアカウントIDで保存する）"""

	def setUp(self):
		from tornado.options import define, options
		from functions.define.base_define import BaseDefine
		from functions.data.control_tinydb import ControlTinyDB
		if "secrets_key" not in options:
			define("secrets_key", default="12345678901234567890123456789012")
		self.ctrl = ControlTinyDB(None)
		for tbl in BaseDefine("define/tables/db_control.xml").dict.values():
			self.ctrl.tables[tbl["id"]] = tbl
		self.ctrl.insert("mst_auth", [{"id": "ref", "name": "参照権限", "function": "参照"}])
		self.ctrl.insert("tbl_account", [{"id": 0, "user_id": "admin", "name": "管理者",
										  "admin": True, "is_active": True}])
		self.ctrl.insert("tbl_auth", [{"id": 0, "function": "参照", "auth_value": True}])
		self.handler = ApiHandler.__new__(ApiHandler)
		self.handler.ctrl_db = {"db_control": self.ctrl}
		self.handler.prm_cmn = {}
		# ログイン時と同じく、DBのアカウントID（数値）でトークンを発行する
		self.token = self.handler.create_token(0)
		self.handler.refresh_token(0, self.token)
		self.handler.request = SimpleNamespace(headers={"Authorization": "Bearer " + self.token})

	def test_check_login_with_numeric_account_id(self):
		"""JWTのsub（文字列）でも、数値のIDで保存したアカウント・権限を取得でき、セッションは無効化されない"""
		self.assertTrue(self.handler.check_login())
		self.assertEqual(self.handler.prm_cmn["account_id"], 0)
		self.assertEqual(self.handler.prm_cmn["account_data"]["user_id"], "admin")
		self.assertEqual(self.handler.prm_cmn["account_auth"], ["参照"])
		self.assertFalse(self.handler.check_token_revoked(self.token))
		# 続くリクエストでも有効なまま
		self.assertTrue(self.handler.check_login())

	def test_check_login_rejects_inactive_account(self):
		"""無効なアカウントのトークンは拒否し、無効化する"""
		self.ctrl.update("tbl_account", {"is_active": False}, {"id": 0})
		self.assertFalse(self.handler.check_login())
		self.assertTrue(self.handler.check_token_revoked(self.token))
