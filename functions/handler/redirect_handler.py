from tornado import web

class RedirectToHttpsHandler(web.RequestHandler):
    def get(self):
        # 301 (永久的) または 302 (一時的) リダイレクト
        self.redirect(self.request.full_url().replace("http:", "https:"), permanent=True)
