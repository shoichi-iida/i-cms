# -*- coding: utf-8 -*-

from tornado import web

class XsrfTokenHandler(web.RequestHandler):
    """
    XSRFトークン生成クラス
    """
    def get(self):
        """
        XSRFトークンをクッキーに設定
        """
        self.set_cookie('xsrf_token', self.xsrf_token)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"xsrf_token": self.xsrf_token.decode('utf-8')}))
        self.finish()
