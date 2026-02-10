# -*- coding: utf-8 -*-

from functions.page.base_page import BasePage

class Page(BasePage):
	def __init__(self):
		super().__init__()
		self.page_handler.extend(["index", "login", "logout", "account_delete"])
		self.need_login = False

	def view(self, handler):
		# ポータル設定読込
		self.check_login(handler)
		lst_menu = []
		for page in handler.pages:
			if page.portal_menu is None:
				continue
			if page.portal_menu.menu_auth is not None and not handler.prm_cmn.get("auth", {}).get(page.portal_menu.menu_auth, False):
				continue
			if page.portal_menu.menu_admin and not handler.prm_cmn.get("admin", False):
				continue
			lst_menu.append(page.portal_menu)
		lst_menu = sorted(lst_menu, key=lambda x: x.menu_order)
		handler.prm_cmn["define_menu"] = lst_menu
		page = handler.prm_req.get("page", "index")
		if page == "login":
			if self.login(handler):
				handler.prm_req["page"] = "index"
				handler.post()
				return
		elif page == "logout":
			self.logout(handler)
		elif page == "account_delete":
			self.account_delete(handler)
			self.logout(handler)
		else:
			self.check_login(handler)

		handler.prm_cmn["lst_notice"] = handler.ctrl_db["db_control"].select("tbl_notice")
		handler.prm_cmn["lst_update"] = handler.ctrl_db["db_control"].select("tbl_update")
		if len(handler.prm_cmn["lst_update"]) > 10:
			handler.prm_cmn["lst_update"] = handler.prm_cmn["lst_update"][:10]
		
		handler.prm_req["page"] = "index"
		super().view(handler)

	def login(self, handler):
		user_id = handler.prm_req.get("user_id", "")
		input_password = handler.prm_req.get("input_password", "")

		# ユーザ情報読込
		data_account = self.get_account(handler, user_id, input_password)
		if data_account is None:
			handler.alert_message("CE-0001")
			return False

		handler.set_cookie_value("account_id", user_id)
		handler.set_cookie_value("account_password", input_password)
		handler.prm_cmn["account_id"] = user_id
		handler.prm_cmn["account_password"] = input_password
		handler.prm_cmn["account_data"] = data_account
		handler.prm_cmn["account_auth"] = self.get_auth(handler, user_id)

		return True

	def logout(self, handler):
		handler.clear_cookie("account_id")
		handler.clear_cookie("account_password")
		handler.prm_cmn["account_id"] = None
		handler.prm_cmn["account_password"] = None
		handler.prm_cmn["account_data"] = None
		handler.prm_cmn["account_auth"] = None

	def account_delete(self, handler):
		handler.ctrl_db["db_control"].delete("tbl_account", [{ "id" : handler.prm_cmn["account_id"] }])
		handler.ctrl_db["db_control"].delete("tbl_auth", [{ "id" : handler.prm_cmn["account_id"] }])
		
		handler.normal_message("C-0001")

