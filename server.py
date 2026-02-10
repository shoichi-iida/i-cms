#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(__file__))
import signal
from glob import glob
from importlib import import_module
from sshtunnel import SSHTunnelForwarder
from tornado import ioloop, httpserver, web, options
from tornado.log import app_log
from tornado.options import options
from functions.common import module_ui
from functions.common.load_options import LoadOptions
from functions.common.control_define import ControlDefine
from functions.handler.redirect_handler import RedirectToHttpsHandler
from functions.handler.main_handler import MainHandler
from functions.handler.socket_handler import SocketHandler
from functions.handler.xsrf_token_handler import XsrfTokenHandler

# 終了確認フラグ
is_closing = False

def print_message(message):
    print(message)
    app_log.info(message)

def signal_handler(signalnum, frame):
    """
    Ctrl+Cによる停止処理

    Parameters
    ----------
    signalnum : int
        シグナル番号
    frame : int
        現在のスタックフレーム
    """
    global is_closing
    print_message("Stopping Server")
    is_closing = True

def try_exit():
    global is_closing
    if is_closing:
        ioloop.IOLoop.current().stop()
        print_message("Server Stopped")

def initialize_pages(path):
    """
    ユーザ定義Pageクラスの読込処理

    Parameters
    ----------
    path : string
        読込を行うユーザ定義Pageクラスの格納パス
    """
    page_dir = glob(path, recursive=True)
    pages = []
    for file_path in page_dir:
        if os.path.basename(file_path) in ["__init__.py", "base_page.py"]:
            continue
        print_message("Loading : {0}".format(file_path))
        class_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.').replace("/", ".")
        print_message("Class Path : {0}".format(class_path))
        pages.append(import_module(class_path).Page())
    return pages

def initialize_sockets(path):
    """
    ユーザ定義Socketクラスの読込処理

    Parameters
    ----------
    path : string
        読込を行うユーザ定義Socketクラスの格納パス
    """
    page_dir = glob(path, recursive=True)
    pages = []
    for file_path in page_dir:
        if os.path.basename(file_path) in ["__init__.py", "base_socket.py"]:
            continue
        print_message("Loading : {0}".format(file_path))
        class_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.').replace("/", ".")
        print_message("Class Path : {0}".format(class_path))
        pages.append(import_module(class_path).Socket())
    return pages

if __name__ == "__main__":
    # 基準パス設定
    base_path = os.path.dirname(__file__)

    # 設定定義
    LoadOptions.define_options()
    # 設定ファイル読込
    LoadOptions.load(base_path)

    # パス設定
    template_path = options.template_path
    static_path = options.static_path
    if not os.path.isabs(template_path):
        template_path = os.path.join(base_path, template_path)
    if not os.path.isabs(static_path):
        static_path = os.path.join(base_path, static_path)

    # 各種表示クラス初期化
    pages = []
    pages.append(initialize_pages("functions/page/main/**/*.py"))
    sockets = []
    sockets.append(initialize_sockets("functions/socket/**/*.py"))
    ctrl_define = ControlDefine()

    # アプリケーション設定
    app = web.Application(
        [
            (r"/", MainHandler, { "pages": pages[0], "ctrl_define": ctrl_define }),
            (r"/socket", SocketHandler, { "sockets": sockets[0], "ctrl_define": ctrl_define }),
            (r"/xsrf-token", XsrfTokenHandler, {}),
        ],
        cookie_secret=options.cookie_key,
        template_path=template_path,
        static_path=static_path,
        xsrf_cookies=True,
        autoescape="xhtml_escape",
        debug=options.debug_mode,
        log_file_prefix=options.log_file_prefix,
        log_rotate_mode=options.log_rotate_mode,
        log_file_max_size=options.log_file_max_size,
        log_file_num_backups=options.log_file_num_backups,
        logging=options.logging,
        ui_modules=module_ui,
        autoreload=not options.multi_thread
    )
    # バーチャルホスト
    # app.add_handlers("sub.example.com", [(r'/', SubHandler)])

    # サーバー設定
    port = options.port
    # SSL使用有無
    if not options.ssl:
        server = httpserver.HTTPServer(app)
    else:
        http_server = httpserver.HTTPServer(web.Application([
            (r"/.*", RedirectToHttpsHandler),
        ]))
        http_server.listen(port)
        port = options.ssl_port
        # SSL設定
        server = httpserver.HTTPServer(
            app,
            ssl_options = {
                "certfile": options.certfile,
                "keyfile": options.keyfile
            }
        )
    # Ctrl+Cによる停止処理
    signal.signal(signal.SIGINT, signal_handler)

    # SSHトンネルの設定
    if options.ssh_tunnel:
        if len(options.remote_bind_port) != len(options.local_bind_port):
            print_message("Error: remote_bind_port and local_bind_port must be same length")
            sys.exit(1)
        print_message(f"Starting SSH Tunnel(host:{options.ssh_host} port:{options.ssh_port})")
        for idx in len(options.remote_bind_port):
            tunnel = SSHTunnelForwarder(
                ssh_address_or_host=(options.ssh_host, options.ssh_port),
                ssh_username=options.ssh_user,
                ssh_password=options.ssh_password,
                remote_bind_address=(options.bind_address, options.remote_bind_port[idx]),
                local_bind_address=("0.0.0.0", options.local_bind_port[idx])
            )
        tunnel.start()

    # マルチスレッド使用有無
    if not options.multi_thread:
        # port設定
        server.listen(port)
    else:
        # port設定
        server.bind(port)
        # スレッド数設定
        server.start(num_processes=options.thread)
    print_message("Starting Server(port:{0})".format(port))

    ioloop.PeriodicCallback(try_exit, 100).start()
    ioloop.IOLoop.current().start()

    if options.ssh_tunnel:
        tunnel.stop()
