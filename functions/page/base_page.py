# -*- coding: utf-8 -*-

from tornado import escape, template
from tornado.options import options
from functions.common.util_encrypt import UtilEncrypt

try:
    if options.ldap:
        from functions.common.control_ldap import ControlLdap
except:
    pass

class BasePage():
    """
    基底画面クラス

    Attributes
    ----------
    handler_url : string
        この画面クラスが処理を行うリクエストURL。
    page_handler : list of string
        この画面クラスが処理を行うリクエストパラメータ「page」のリスト。
    portal_menu : functions.define.menu_define.MenuDefine
        ポータル画面に表示するメニュー設定。
        ※何も表示しない場合はNoneを設定。
    maintenance_use : boolean
        この画面はメンテナンス中にも使用可能か否か。
        True : 可能
        False : 不可能
    need_login : boolean
        この画面の表示はログインが必要か否か。
        True : 必要
        False : 不要
    back_page : string
        need_loginが"True"でログインされていない際に表示する画面の「page」。
    need_auth : string
        この画面の表示に必要な権限名。
    back_page_auth : string
        need_auth"True"で権限を持っていない際に表示する画面の「page」。
    page_dir : string
        templateの基準ディレクトリ。
        ※通常画面であれば"template/main"から先、管理画面であれば"template/admin"から先を指定すること。
    """
    def __init__(self):
        """
        コンストラクタ
        """
        self.handler_url = ""
        self.page_handler = []
        self.portal_menu = None
        self.maintenance_use = False
        self.need_login = True
        self.back_page = "index"
        self.need_auth = ""
        self.back_page_auth = "index"
        self.page_dir = None

    def set_req(self, handler, keys, i):
        prm_req = handler.prm_req
        dict = {}
        for key in keys:
            if "ref" in key[2]:
                val = prm_req.get("{0}_{1}".format(key[0], i), None)
                if key[1] == "text":
                    if val is None:
                        val = ""
                    else:
                        val = escape.xhtml_unescape(val)
                elif key[1] == "int":
                    if val is None or val == "":
                        val = ""
                    else:
                        val = int(val)
                dict[key[0]] = val
            if "up" in key[2]:
                val = prm_req.get("B_{0}_{1}".format(key[0], i), None)
                if key[1] == "text":
                    if val is None:
                        val = ""
                    else:
                        val = escape.xhtml_unescape(val)
                elif key[1] == "int":
                    if val is None or val == "":
                        val = ""
                    else:
                        val = int(val)
                dict["B_{0}".format(key[0])] = val
        return dict

    def get_view(self, handler):
        self.view(handler)

    def post_view(self, handler):
        self.view(handler)

    def put_view(self, handler):
        self.view(handler)

    def delete_view(self, handler):
        self.view(handler)

    def view(self, handler):
        lst_obj = []
        if handler.prm_cmn.get("define_page") is not None:
            self.append_obj(handler.prm_cmn["define_page"].get("obj", []), lst_obj)
            t = template.Template("\r\n".join(lst_obj))
            handler.prm_cmn["generate_page"] = t.generate(prm_cmn=handler.prm_cmn, prm_req=handler.prm_req, ctrl_define=handler.ctrl_define)
        base_dir = ""
        if self.page_dir is not None:
            base_dir = "{0}/".format(self.page_dir)
        handler.set_header("content-security-policy", "default-src 'self' 'unsafe-inline' 'unsafe-eval'; base-uri 'self';")
        handler.render(
            "main/{0}{1}.html".format(
                base_dir,
                handler.prm_req.get("page", "index")
            ),
            prm_cmn=handler.prm_cmn,
            prm_req=handler.prm_req,
            ctrl_define=handler.ctrl_define
        )

    def append_obj(self, define, lst_obj):
        for obj in define:
            # 開きタグ
            if obj.get("tag", "") != "":
                tag = []
                tag.append("<{0}".format(obj["tag"]))
                for k in obj.keys():
                    if k in ["tag", "html", "obj"]:
                        continue
                    if obj.get(k, "") != "":
                        tag.append(" {0}=\"{1}\"".format(k, obj[k]))
                    else:
                        tag.append(" {0}".format(k))
                tag.append(">")
                lst_obj.append("".join(tag))
            # innerHTML
            if obj.get("html", "") != "":
                lst_obj.append(obj["html"])
            # 子要素
            self.append_obj(obj.get("obj", []), lst_obj)
            # 閉じタグ
            if obj.get("tag", "") != "":
                lst_obj.append("</{0}>".format(
                    obj["tag"]
                ))

    def check_ldap(self, user_id, user_password):
        # LDAP情報読込
        if not options.ldap:
            return None
        ldap = ControlLdap()
        if not ldap.connect("ht", user_id, user_password):
            return None
        return ldap.get_user_name()

    def check_login(self, handler):
        user_id = handler.prm_cmn.get("account_id", "")
        input_password = handler.prm_cmn.get("account_password", "")

        # ユーザ情報読込
        data_account = self.get_account(handler, user_id, input_password)
        if data_account is not None:
            handler.prm_cmn["admin"] = data_account["admin"]
        else:
            return False

        handler.prm_cmn["auth"] = self.get_auth(handler, user_id)
        handler.prm_cmn["account_id"] = user_id
        handler.prm_cmn["account_password"] = input_password
        handler.prm_cmn["account_data"] = data_account
        handler.prm_cmn["account_auth"] = self.get_auth(handler, user_id)
        handler.prm_cmn["account_settings"] = self.get_account_settings(handler, user_id)

        return True

    def admin_view(self, handler):
        handler.render("admin/{0}.html".format(handler.prm_req.get("page", "index")), prm_cmn=handler.prm_cmn, prm_req=handler.prm_req)

    def admin_check_login(self, handler):
        user_id = handler.prm_cmn.get("admin_account_id", "")
        input_password = handler.prm_cmn.get("admin_account_password", "")

        # ユーザ情報読込
        data_account = self.get_account(handler, user_id, input_password)
        if data_account is None:
            return False
        handler.prm_cmn["admin_account_name"] = data_account["name"]
        handler.prm_cmn["admin"] = data_account["admin"]
        handler.prm_cmn["admin_login"] = True

        handler.prm_cmn["auth"] = self.get_auth(handler, user_id)
        handler.prm_cmn["account_settings"] = self.get_account_settings(handler, user_id)

        return True

    def get_account(self, handler, user_id, input_password):
        result = handler.ctrl_db["db_control"].select("tbl_account", dict_select={
            "id": user_id,
            "password": UtilEncrypt.encrypt_xor(input_password, options.encrypt_key)
        })
        if len(result) == 0:
            return None
        return result[0]

    def get_auth(self, handler, user_id):
        result = {}
        for record in handler.ctrl_db["db_control"].select("tbl_auth", dict_select={
            "id": user_id
        }):
            result[record["function"]] = record["auth_value"]
        lst_affiliation = []
        for r in handler.ctrl_db["db_control"].select("tbl_group_affiliation", dict_select={
            "account_id": user_id
        }):
            lst_affiliation.append(r["group_id"])
        for group_id in lst_affiliation:
            for record in handler.ctrl_db["db_control"].select("tbl_group_auth", dict_select={
                "id": group_id
            }):
                if record["auth_value"]:
                    result[record["function"]] = record["auth_value"]
        return result

    def get_account_settings(self, handler, user_id):
        result = {}
        for r in handler.ctrl_db["db_control"].select("tbl_account_settings", dict_select={
            "id": user_id
        }):
            result[r["setting_key"]] = r["setting_value"]
        return result
