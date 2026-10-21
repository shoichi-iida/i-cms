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
	def __init__(self, events, commit_error=None):
		self.events = events
		self.commit_error = commit_error
		self.serialize_writes = False

	def begin(self, serialize=None):
		self.events.append("begin")

	def commit(self):
		self.events.append("commit")
		if self.commit_error is not None:
			raise self.commit_error

	def rollback(self):
		self.events.append("rollback")

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
