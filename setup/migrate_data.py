#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
既存DBのデータ移行スクリプト

テーブル定義（define/tables）の変更前に作成したDBを、現在の定義で使えるようにする
（列の追加・権限キーの変換・パスワードのbcryptへの移行。詳細はMigrateDataを参照）。

サーバーを停止した状態で、リポジトリのルートから実行すること。
server.iniのencrypt_keyは、パスワードをXOR暗号化していたときの値のまま変更しないこと。

    python setup/migrate_data.py
"""

import os
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(base_path)
from tornado.options import options
from functions.common.load_options import LoadOptions
from functions.common.control_define import ControlDefine
from functions.common.control_db import ControlDB
from functions.common.migrate_data import MigrateData

if __name__ == "__main__":
	# 定義体・DBの相対パスをserver.pyと同じく基準パスから解決する
	os.chdir(base_path)

	# 設定読込
	LoadOptions.define_options()
	LoadOptions.load(base_path)

	ctrl_define = ControlDefine()
	ctrl_db = ControlDB(ctrl_define)
	failed = MigrateData(ctrl_db, ctrl_define, options.encrypt_key).exec()
	if len(failed) > 0:
		print("パスワードを移行できなかったアカウントがあります。encrypt_keyを確認してください。")
		sys.exit(1)
	print("移行が完了しました。")
