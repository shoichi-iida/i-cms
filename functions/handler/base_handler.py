# -*- coding: utf-8 -*-

import json
import uuid
import jwt
import traceback
import threading
import urllib.parse
import pyotp
import qrcode
import base64
from io import BytesIO
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from tornado import web, escape
from tornado.options import options
from tornado.log import app_log
from functions.common.initialize_data import InitializeData
from functions.common.control_db import ControlDB
from functions.common.util_encrypt import UtilEncrypt

try:
    if options.ldap:
        from functions.common.control_ldap import ControlLdap
except:
    pass

class BaseHandler(web.RequestHandler):
    """
    HTTPリクエスト処理基底クラス

    Attributes
    ----------
    pages : list of functions.page.*.Page
        ユーザ定義Pageクラスの配列
    admin_page : boolean
        本リクエスト処理が管理者用機能であるかどうかを示す。
        True : 管理者用
        False : 一般利用者用
    ctrl_define : functions.common.control_define.ControlDefine
        定義管理クラス
    ctrl_db : functions.common.control_db.ControlDB
        DB管理クラス
    lock : threading.Lock
        排他制御オブジェクト
    prm_cmn : dict
        汎用パラメータ格納辞書
    prm_get : dict
        GETパラメータ格納辞書
    prm_req : dict
        POSTパラメータ格納辞書

    Notes
    -----
    新たなリクエスト処理を実装する場合、本クラスを継承して実装すること。
    """

    def initialize(self, pages, ctrl_define):
        """
        初期化処理

        Parameters
        ----------
        pages : Pageクラス配列
            ユーザ定義Pageクラスの配列
        ctrl_define : ControlDefine
            定義管理クラス
        """
        self.pages = pages
        self.ctrl_define = ctrl_define
        self.lock = threading.Lock()

        # 汎用パラメータ
        self.prm_cmn = OrderedDict()
        # GETパラメータ
        self.prm_get = OrderedDict()
        # リクエストパラメータ
        self.prm_req = OrderedDict()

    def prepare(self):
        """
        リクエスト前処理
        """
        # DB制御
        self.ctrl_db = ControlDB(self.ctrl_define)

        # 基本設定読込
        self.prm_cmn["use_ssl"] = options.ssl
        if not options.ssl:
            self.prm_cmn["http_host"] = "http://{0}:{1}".format(options.host_name, options.port)
            self.prm_cmn["websocket_host"] = "ws://{0}:{1}".format(options.host_name, options.port)
        else:
            self.prm_cmn["http_host"] = "https://{0}:{1}".format(options.host_name, options.ssl_port)
            self.prm_cmn["websocket_host"] = "wss://{0}:{1}".format(options.host_name, options.ssl_port)
        self.prm_cmn["use_ldap"] = options.ldap

        self.is_error = False
        try:
            for param in self.request.query.split("&"):
                if param == "":
                    break
                key = param.split("=")
                self.prm_get[key[0]] = self.get_query_arguments(key[0])
            for key in self.request.arguments.keys():
                self.prm_req[key] = self.get_arguments(key)
            self.prm_file = OrderedDict()
            for key in self.request.files.keys():
                self.prm_file[key] = self.request.files[key]

            # 初期設定の要否確認
            self.lock.acquire()
            try:
                tbl_setting = self.ctrl_db["db_control"].select("tbl_setting")
            except:
                tbl_setting = []
            if len(tbl_setting) == 0:
                InitializeData.exec(self)
            self.lock.release()

            for record in self.ctrl_db["db_control"].select("tbl_setting"):
                self.prm_cmn[record["setting_key"]] = record["setting_value"]
        except Exception as e:
            self.append_log(self.prm_cmn.get("account_id", "Non Login"), "alert")
            print(e)
            self.append_message("CE-9999", [str(e)])
            for m in str(traceback.format_exc()).splitlines():
                print(m)
                self.append_message("", [m], "alert")
            self.is_error = True

    def on_finish(self):
        """
        リクエスト後処理
        """
        # DBクローズ
        self.ctrl_db.__del__()

    def get(self, *args):
        """
        GETリクエスト処理
        """
        self.proc_access("get", args)

    def post(self, *args):
        """
        POSTリクエスト処理
        """
        self.proc_access("post", args)

    def put(self, *args):
        """
        PUTリクエスト処理
        """
        self.proc_access("put", args)

    def patch(self, *args):
        """
        PUTリクエスト処理
        """
        self.proc_access("patch", args)

    def delete(self, *args):
        """
        DELETEリクエスト処理
        """
        self.proc_access("delete", args)

    def proc_access(self, mode, args):
        """
        共通リクエスト処理
        """
        pass

    def view_download(self, file_name, file_io):
        """
        ファイルダウンロード応答処理

        Parameters
        ----------
        file_name : string
            ダウンロードファイル名
        file_io : FileIO
            ダウンロード対象のファイルIOオブジェクト
        """
        self.set_header("Content-Type", "application/octet-stream")
        quote_name = urllib.parse.quote(file_name)
        self.set_header("Content-Disposition", f"attachment; filename={quote_name}; filename*=UTF-8'{quote_name}'")
        buf_size = 4096
        while True:
            data = file_io.read(buf_size)
            if not data:
                break
            self.write(data)
        self.finish()

    def view_json(self, resonse):
        """
        JSON応答処理

        Parameters
        ----------
        response : dict
            応答電文
        """
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(resonse))
        self.finish()

    def raise_http_error(self, http_cd, msg_cd, msg_prm=[]):
        """
        HTTPエラー例外生成処理
        
        Parameters
        ----------
        http_cd : int
            HTTPステータスコード
        msg_cd : string
            メッセージコード
        msg_prm : array, default []
            メッセージ付随情報
        
        Raises
        -------
        HTTPError
            HTTPエラー例外
        """
        raise web.HTTPError(http_cd, log_message=self.get_message(msg_cd, msg_prm))

    def set_http_status(self, http_cd, msg_cd=None, msg_prm=[]):
        """
        HTTPステータスコード設定処理
        
        Parameters
        ----------
        http_cd : int
            HTTPステータスコード
        msg_cd : string
            メッセージコード
        msg_prm : array, default []
            メッセージ付随情報
        """
        self.set_status(http_cd, reason=self.get_message(msg_cd, msg_prm) if msg_cd is not None else None)
    
    def return_error_json(self, http_cd, msg_cd, msg_prm=[], details={}):
        """
        エラー発生時用JSON応答処理
        
        Parameters
        ----------
        http_cd : int
            HTTPステータスコード
        msg_cd : string
            メッセージコード
        msg_prm : array, default []
            メッセージ付随情報
        details :  dict, default {}
            エラー詳細を項目名(key)と詳細(value)で定義
        """
        self.set_http_status(http_cd, msg_cd, msg_prm)
        result = {
            "error": {
                "code": msg_cd,
                "message": self.get_message(msg_cd, msg_prm)
            }
        }
        if len(details.keys()) > 0:
            result["error"]["details"] = []
            for key in details.keys():
                result["error"]["details"].append({
                    "field": key,
                    "message": details[key]
                })
        self.view_json(result)

    def create_token(self, param):
        """
        トークン生成処理
        
        Parameters
        ----------
        param : string
            トークン付随情報
        
        Returns
        -------
        string
            トークン
        """
        payload = {
            "sub": str(param),
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
            "iat": datetime.now(timezone.utc)
        }
        return jwt.encode(payload, options.secrets_key, algorithm="HS256")

    def extract_token(self):
        """
        トークン取得処理
        
        Returns
        -------
        string
            トークン
        """
        header = self.request.headers.get("Authorization", "")
        return header.removeprefix("Bearer ") or None

    def verify_token(self, token):
        """
        トークン認証処理
        
        Parameters
        ----------
        token : string
            トークン
        
        Returns
        -------
        boolean
            認証結果(True : 認証成功、False : 認証失敗)
        """
        return jwt.decode(token, options.secrets_key, algorithms=["HS256"]).get("sub")
    
    def refresh_token(self, user_id, token):
        """
        トークン有効期限更新処理
        
        Parameters
        ----------
        user_id : user_id
            ユーザーID
        token : string
            トークン
        """
        self.ctrl_db["db_control"].insert("tbl_account_token", [{
            "id": user_id,
            "token": token,
            "expires_at": (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "revoked": False
        }], True)
    
    def invalidate_token(self, user_id):
        """
        トークン無効化処理
        
        Parameters
        ----------
        token : user_id
            ユーザーID
        """
        self.ctrl_db["db_control"].update("tbl_account_token", {
            "revoked": True
        }, {
            "id": user_id
        })

    def check_token_revoked(self, token):
        """
        トークン無効状態確認処理
        
        Parameters
        ----------
        token : string
            トークン
        
        Returns
        -------
        boolean
            確認結果(True : 無効化済み、False : 有効)
        """
        result = self.ctrl_db["db_control"].select("tbl_account_token", dict_select={
            "token": token
        })
        if len(result) == 0 or result[0]["revoked"] or datetime.strptime(result[0]["expires_at"], "%Y-%m-%d %H:%M:%S") < datetime.now():
            return True
        return False
    
    def create_secret(self):
        """
        シークレットキー生成処理
        
        Returns
        -------
        string
            シークレットキー
        """
        return pyotp.random_base32()
    
    def create_secret_uri(self, secret, user_id):
        """
        シークレット登録用URI生成処理
        
        Parameters
        ----------
        secret : string
            シークレットキー
        user_id : string
            ユーザーID
        
        Returns
        -------
        string
            シークレット登録用URI
        """
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(name=user_id, issuer_name=options.site_title)
    
    def create_qrcode(self, data):
        """
        QRコード生成処理
        
        Parameters
        ----------
        data : string
            QRコード格納データ
        
        Returns
        -------
        string
            Base64エンコードされたURI
        """
        qr = qrcode.QRCode(box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")
    
    def verify_secret(self, secret, code):
        """
        MFAコード認証処理
        
        Parameters
        ----------
        secret : string
            シークレットキー
        code : string
            クライアントから受け取ったコード
        
        Returns
        -------
        boolean
            認証結果(True : 認証成功、False : 認証失敗)
        """
        totp = pyotp.TOTP(secret)
        return totp.verify(code)

    def get_cookie_value(self, key):
        """
        cookie取得処理

        Parameters
        ----------
        key : string
            cookieのキー値
        """
        value = self.get_signed_cookie(key)
        if not value: return None
        return escape.xhtml_unescape(value)

    def set_cookie_value(self, key, value):
        """
        cookie設定処理

        Parameters
        ----------
        key : string
            cookieのキー値
        value : string
            cookieの設定値
        """
        self.set_signed_cookie(key, escape.xhtml_escape(value))

    def get_message(self, msg_cd, msg_prm=[]):
        """
        メッセージ取得処理

        Parameters
        ----------
        msg_cd : string
            メッセージID
        msg_prm : list of string ,default []
            メッセージに埋め込むパラメータ配列。

        Returns
        -------
        string
            フォーマットされたメッセージ文字列。
        """
        def_message = self.ctrl_define["message"]["def"].get(msg_cd, {})
        return def_message.get("text", f"[{msg_cd}] メッセージ未定義").format(msg_prm)

    def append_message(self, msg_cd, msg_prm=[], msg_type="normal"):
        """
        メッセージ追加処理（基底実装）

        PageHandler等のサブクラスで画面表示用にオーバーライドされる。
        基底クラスではログ出力のみ行う。

        Parameters
        ----------
        msg_cd : string
            メッセージコード
        msg_prm : list of string, default []
            メッセージに埋め込むパラメータ配列
        msg_type : string, default normal
            メッセージ種別(alert,warning,normal,debug)
        """
        message = self.get_message(msg_cd, msg_prm) if msg_cd else " ".join(msg_prm)
        self.append_log(message, msg_type)

    def append_log(self, message, msg_type="normal"):
        """
        APPログ出力処理

        Parameters
        ----------
        message : string
            出力するログメッセージ。
        msg_type : string, default normal
            ログレベル(alert,warning,normal,debug)。
        """
        if msg_type == "alert":
            app_log.error(message)
        elif msg_type == "warning":
            app_log.warning(message)
        elif msg_type == "normal":
            app_log.info(message)
        else:
            app_log.debug(message)

    def append_access_hist(self):
        """
        操作履歴追加処理

        Parameters
        ----------
        op : string
            操作名
        return_op : string
            表示操作
        """
        access_data = {
            "access_date": datetime.now().strftime("%Y-%m-%d"),
            "account_id": self.prm_cmn["account_id"]
        }
        if len(self.ctrl_db["db_control"].select("tbl_access_hist", access_data)) > 0:
            return
        self.ctrl_db["db_control"].begin()
        self.ctrl_db["db_control"].insert("tbl_access_hist", [access_data])
        self.ctrl_db["db_control"].commit()

    def get_operation_id(self):
        """
        操作ID取得処理

        Returns
        -------
        string
            操作ID
        """
        operation_id = self.prm_req.get("_xsrf")
        if operation_id is None:
            operation_id = uuid.uuid4().hex
        else:
            operation_id = operation_id.split("|")[2]
        return operation_id

    def get_operation(self):
        """
        操作履歴取得処理
        """
        operation_id = self.get_operation_id()
        return self.ctrl_db["db_control"].select("tbl_operation_hist", { "operation_id": operation_id })

    def append_operation(self, op, return_op):
        """
        操作履歴追加処理

        Parameters
        ----------
        op : string
            操作名
        return_op : string
            表示操作
        """
        self.ctrl_db["db_control"].begin()
        operation_id = self.get_operation_id()
        operation_data = {
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation_id": operation_id,
            "account_id": self.prm_cmn["account_id"],
            "operation": op,
            "return_operation": return_op,
            "args": json.dumps(self.prm_req)
        }
        self.ctrl_db["db_control"].insert("tbl_operation_hist", [operation_data])
        self.ctrl_db["db_control"].commit()

    def get_auth_list(self):
        result = {}
        for record in self.ctrl_db["db_control"].select("mst_auth"):
            result[record["id"]] = {
                "id": record["id"],
                "name": record["name"],
                "ref_name": record["function"]
            }
        return result

    def check_ldap(self, user_id, user_password):
        # LDAP情報読込
        if not options.ldap:
            return None
        ldap = ControlLdap()
        if not ldap.connect("ht", user_id, user_password):
            return None
        return ldap.get_user_name()

    def check_login(self):
        # トークン取得
        token = self.extract_token()
        if not token:
            return False
        
        # トークン検証
        try:
            if self.check_token_revoked(token):
                return False
            user_id = self.verify_token(token)
            if user_id is None:
                return False
        except jwt.PyJWTError:
            return False

        self.refresh_token(user_id, token)

        # ユーザ情報読込
        data_account = self.ctrl_db["db_control"].select("tbl_account", dict_select={
            "id": user_id
        })
        if len(data_account) > 0:
            self.prm_cmn["admin"] = data_account[0]["admin"]
        else:
            return False

        self.prm_cmn["account_id"] = user_id
        self.prm_cmn["token"] = token
        self.prm_cmn["account_data"] = data_account[0]
        self.prm_cmn["account_auth"] = [k for k, v in self.get_account_auth(user_id).items() if v is True or v == 1]
        self.prm_cmn["account_settings"] = self.get_account_settings(user_id)

        return True

    def get_account(self, user_id, input_password):
        result = self.ctrl_db["db_control"].select("tbl_account", dict_select={
            "user_id": user_id,
            "password": UtilEncrypt.encrypt_xor(input_password, options.encrypt_key)
        })
        if len(result) == 0:
            return None
        return result[0]

    def get_account_auth(self, user_id):
        result = {}
        for record in self.ctrl_db["db_control"].select("tbl_auth", dict_select={
            "id": user_id
        }):
            result[record["auth_id"]] = record["auth_value"]
        lst_affiliation = []
        for r in self.ctrl_db["db_control"].select("tbl_group_affiliation", dict_select={
            "account_id": user_id
        }):
            lst_affiliation.append(r["group_id"])
        for group_id in lst_affiliation:
            for record in self.ctrl_db["db_control"].select("tbl_group_auth", dict_select={
                "id": group_id
            }):
                if record["auth_value"]:
                    result[record["auth_id"]] = record["auth_value"]
        return result

    def get_account_settings(self, user_id):
        result = {}
        for r in self.ctrl_db["db_control"].select("tbl_account_settings", dict_select={
            "id": user_id
        }):
            result[r["setting_key"]] = r["setting_value"]
        return result
