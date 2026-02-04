# -*- coding: utf-8 -*-

from functions.page.base_page import BasePage

class Page(BasePage):
	"""
	サンプル画面クラス
	"""
	def __init__(self):
		"""
		コンストラクタ
		"""
		super().__init__()
		# form.pageに指定された値が下記設定値と合致する場合、このクラスを用いて処理を実施する
		self.page_handler.extend(["sample1", "sample2"])
		# ログイン状態である必要があるか
		self.need_login = False
		# need_login=Trueでログインしていなかった場合に表示されるページ
		self.back_page = "index"
		# このクラスを実行するのに必要な権限設定。define/common/auth.xmlで定義
		self.need_auth = ""
		# 権限不足で表示されるページ
		self.back_page_auth = "index"
		# templateファイルの格納ディレクトリ
		self.page_dir = "sample"

	def post_view(self, handler):
		# form.pageの値を取得(値が取れなかった場合sample1とする)
		page = handler.prm_req.get("page", "sample1")
		# page分岐
		if page == "sample1":
			# メッセージを表示する場合はappend_messageを実行
			handler.append_message("C-XXXX", ["テスト"])
		elif page == "sample2":
			# 別クラスで表示処理を実行した場合はpageを設定してpost()を実行
			# この時、絶対returnする事。
			handler.prm_req["page"] = "index"
			# エラーメッセージを表示する場合にもappend_messageを実行
			# ※メッセージ定義からエラーレベルを自動検出している。
			handler.append_message("CE-XXXX", ["テスト", "テスト2"])
			handler.post()
			return
		else:
			pass
		
		# pageと同名のhtmlファイルを使用する為、処理毎に用意していない場合は共通のページ名を設定する
		handler.prm_req["page"] = "index"
		# ページ表示
		super().view(handler)
