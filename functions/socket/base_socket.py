# -*- coding: utf-8 -*-

class BaseSocket():
    """
    基底ソケットクラス

    Attributes
    ----------
    handler_url : string
        このソケットクラスが処理を行うリクエストURL。
    page_handler : list of string
        このソケットクラスが処理を行うリクエストパラメータ「mode」のリスト。
    """
    def __init__(self):
        """
        コンストラクタ
        """
        self.handler_url = "socket"
        self.socket_handler = []

    def exec_process(self, handler, message):
        pass
