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
from functions.handler.page_handler import PageHandler
from functions.handler.api_handler import ApiHandler
from functions.handler.socket_handler import SocketHandler
from functions.handler.xsrf_token_handler import XsrfTokenHandler

# SSHトンネル
tunnels = []

def print_message(message):
    print(message)
    app_log.info(message)

def signal_handler(signal, frame):
    """
    Ctrl+Cによる停止処理

    Parameters
    ----------
    signal : int
        シグナル番号
    frame : int
        現在のスタックフレーム
    """
    print_message("Stopping Server")
    ioloop.IOLoop.current().add_callback_from_signal(shutdown)

def shutdown():
    """
    サーバ停止処理
    """
    ioloop.IOLoop.current().stop()
    print_message("Server Stopped")
    if len(tunnels) == 0:
        return
    for tunnel in tunnels:
        tunnel.stop()
    print_message(f"Stopped SSH Tunnel")

def initialize_pages(path):
    """
    ユーザ定義Pageクラスの読込処理

    Parameters
    ----------
    path : string
        読込を行うユーザ定義Pageクラスの格納パス
    """
    page_dir = glob(path, recursive=True)
    result = {}
    for file_path in page_dir:
        if os.path.basename(file_path) in ["__init__.py", "base_page.py"]:
            continue
        print_message("Loading : {0}".format(file_path.replace("\\", "/")))
        class_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.').replace("/", ".")
        print_message("Class Path : {0}".format(class_path))
        page = import_module(class_path).Page()
        if not page.handler_url in result.keys():
            result[page.handler_url] = []
        result[page.handler_url].append(page)
    return result


def initialize_api(path):
    """
    ユーザ定義APIクラスの読込処理

    Parameters
    ----------
    path : string
        読込を行うユーザ定義Pageクラスの格納パス
    """
    page_dir = glob(path, recursive=True)
    result = {}
    for file_path in page_dir:
        if os.path.basename(file_path) in ["__init__.py", "base_api.py"]:
            continue
        print_message("Loading : {0}".format(file_path.replace("\\", "/")))
        class_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.').replace("/", ".")
        print_message("Class Path : {0}".format(class_path))
        api = import_module(class_path).Api()
        if not api.handler_url in result.keys():
            result[api.handler_url] = []
        result[api.handler_url].append(api)
    return result

def initialize_sockets(path):
    """
    ユーザ定義Socketクラスの読込処理

    Parameters
    ----------
    path : string
        読込を行うユーザ定義Socketクラスの格納パス
    """
    page_dir = glob(path, recursive=True)
    result = {}
    for file_path in page_dir:
        if os.path.basename(file_path) in ["__init__.py", "base_socket.py"]:
            continue
        print_message("Loading : {0}".format(file_path.replace("\\", "/")))
        class_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.').replace("/", ".")
        print_message("Class Path : {0}".format(class_path))
        socket = import_module(class_path).Socket()
        if not socket.handler_url in result.keys():
            result[socket.handler_url] = []
        result[socket.handler_url].append(socket)
    return result

def make_app(base_path=None):
    """
    Tornado Applicationの構築処理

    server.py本体（__main__）と結合テスト（tests_integration）の双方から
    同一構成のApplicationを生成するための共通処理。
    事前にLoadOptionsで設定が読み込まれていること。

    Parameters
    ----------
    base_path : string, default None
        基準パス。未指定の場合は本ファイルの格納ディレクトリ。

    Returns
    -------
    tornado.web.Application
        構築済みApplication
    """
    if base_path is None:
        base_path = os.path.dirname(__file__)

    # パス設定
    template_path = options.template_path
    static_path = options.static_path
    if not os.path.isabs(template_path):
        template_path = os.path.join(base_path, template_path)
    if not os.path.isabs(static_path):
        static_path = os.path.join(base_path, static_path)

    # 各種表示クラス初期化
    pages = initialize_pages("functions/page/**/*.py")
    api = initialize_api("functions/api/**/*.py")
    sockets = initialize_sockets("functions/socket/**/*.py")
    if len(pages.keys() & api.keys() & sockets.keys() & {"xsrf-token"}) > 0:
        print_message("Error : Duplicate URL")
        sys.exit(1)
    ctrl_define = ControlDefine()

    # URL設定
    urls = [(r"/xsrf-token", XsrfTokenHandler, {})]
    for url in pages.keys():
        print_message("Starting PageHandler : {0}".format(url))
        urls.append((r"{0}".format(url), PageHandler, { "pages": pages[url], "ctrl_define": ctrl_define }))
    for url in api.keys():
        print_message("Starting ApiHandler : {0}".format(url))
        urls.append((r"{0}".format(url), ApiHandler, { "api": api[url], "ctrl_define": ctrl_define }))
    for url in sockets.keys():
        print_message("Starting SocketHandler : {0}".format(url))
        urls.append((r"{0}".format(url), SocketHandler, { "sockets": sockets[url], "ctrl_define": ctrl_define }))

    # アプリケーション設定
    return web.Application(
        urls,
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
        autoreload=options.debug_mode and not options.multi_thread
    )

if __name__ == "__main__":
    # 基準パス設定
    base_path = os.path.dirname(__file__)

    # 設定定義
    LoadOptions.define_options()
    # 設定ファイル読込
    LoadOptions.load(base_path)

    # アプリケーション構築
    app = make_app(base_path)

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
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # SSHトンネルの設定
    if options.ssh_tunnel:
        if len(options.remote_bind_port) != len(options.local_bind_port):
            print_message("Error : remote_bind_port and local_bind_port must be same length")
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
            tunnels.append(tunnel)

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

    try:
        ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        print_message("Stopping Server")
        shutdown()
