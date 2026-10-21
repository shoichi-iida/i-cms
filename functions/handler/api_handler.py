# -*- coding: utf-8 -*-

import json
import traceback
from functions.handler.base_handler import BaseHandler
from functions.common.except_common import ExceptCommon
from tornado.log import app_log
from tornado import web

class ApiHandler(BaseHandler):
    # 500応答（CE-9999）の詳細に埋め込む文言。
    # 例外の内容はサーバー側のログにのみ残し、レスポンスへは含めない。
    INTERNAL_ERROR_DETAIL = "管理者にお問い合わせください。"

    def initialize(self, api, ctrl_define):
        try:
            super().initialize(api, ctrl_define)
            # ログイン状態管理
            self.prm_cmn["account_id"] = self.get_cookie_value("account_id")
            self.prm_cmn["login_token"] = self.get_cookie_value("login_token")
            self.prm_cmn["login"] = False
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            app_log.error(e)
            app_log.error(traceback.format_exc())

    def prepare(self):
        try:
            super().prepare()
            self.prm_body = self.load_json_body()
        except web.HTTPError as e:
            # 400はリクエストが不正なUTF-8を含む場合（クエリ文字列・フォームの値は
            # BaseHandler.prepare、JSONのボディはload_json_body）と、
            # JSONのボディがオブジェクトでない場合（load_json_body）。
            # TornadoのHTMLのエラーページではなく、他のバリデーションエラーと同じ
            # 400 CE-0011のJSONで返す。全APIの前処理のため、
            # ログイン・権限の判定（proc_access）より先に判定される
            if e.status_code != 400:
                raise
            # 診断用にTornadoのメッセージ（クエリ文字列・フォームは項目名と値の先頭40バイト、
            # JSONのボディは復号できなかったバイトと位置）を残す。
            # e.log_messageは互換用のプロパティで、args無しなら「%」を「%%」に
            # エスケープし、args有りなら未展開のテンプレートを返すため、
            # 展開済みの文字列になるstr(e)を使う
            self.append_log(str(e), "warning")
            self.return_error_json(400, "CE-0011", [])
            # 応答済みのため後続処理（proc_access）を行わせない（check_xsrf_cookieと同じ）
            raise web.Finish()

    def load_json_body(self):
        """
        リクエストボディ（JSON）の読込

        Returns
        -------
        prm_body : dict
            JSONとして解析した値（オブジェクト）。ボディが空・JSONとして不正な場合は空のdict
        """
        try:
            body = json.loads(self.request.body)
        except json.JSONDecodeError:
            return {}
        except UnicodeDecodeError as e:
            # json.loadsはbytesをUTF-8（UTF-16・UTF-32はバイトの並びから判定）として
            # 復号してから解析する。復号の失敗はJSONDecodeErrorではなくUnicodeDecodeError。
            # 空のボディとして扱うとボディを使わないAPIが成功してしまうため、
            # クエリ文字列の値と同じ400のHTTPError（Tornadoのdecode_argumentと同じ形）とする
            raise web.HTTPError(400, "Invalid unicode in request body: {0}".format(e))
        if not isinstance(body, dict):
            # オブジェクトでないJSON（null・配列・数値・文字列・true/false）。各APIは
            # prm_bodyをdictとして読む（.get()・in）ため、そのまま渡すとAttributeErrorの
            # 500になる（文字列・配列では in が部分一致・要素の判定になる）。空のボディとして
            # 扱うとボディを使わないAPIが成功してしまうため、不正なUTF-8と同じ
            # 400のHTTPErrorとする。ボディの内容（パスワード等を含み得る）はログに出さず、型名のみ残す
            raise web.HTTPError(
                400, "Request body is not a JSON object: {0}".format(type(body).__name__))
        return body

    def check_xsrf_cookie(self):
        """
        XSRFトークンの検証（非GETリクエスト）

        Tornado既定の失敗応答はHTMLのエラーページであり、エラーコードを持たない。
        APIの応答は全てJSON（return_error_json）で統一し、フロント側が
        「XSRFトークンの期限切れ（再取得してリトライすべき）」と
        「権限による403（リトライしても変わらない）」を
        エラーコードで区別できるようにする。
        """
        try:
            super().check_xsrf_cookie()
        except web.HTTPError:
            self.return_error_json(403, "CE-0011", [])
            # 応答済みのため後続処理（prepare・proc_access）を行わせない。
            # web.Finishはエラー応答を上書きしない（send_errorを呼ばない）
            raise web.Finish()

    # HTTPメソッド（proc_accessのmode）→ APIの処理メソッド名
    API_METHODS = {
        "get": "get_response",
        "post": "post_response",
        "put": "put_response",
        "patch": "patch_response",
        "delete": "delete_response",
    }

    def call_api(self, page, mode, args):
        """
        APIの処理を呼び出す。更新系（GET以外）は、書き込みを直列化したトランザクションの中で
        呼び出す（issue #207）。

        更新系のAPIは「検証 → トランザクション → 書き込み」の形で、検証とコミットの間に
        別のプロセス（server.py は複数プロセスで動く）が状態を変えると検出できなかった。
        リクエストの最初に直列化したトランザクションを開始し
        （SQLite: BEGIN IMMEDIATE、MySQL: 書き込みロック）、検証から書き込みまでを
        ほかの更新系のリクエストと重ならないようにする。

        - APIの中の begin() は、既に始めたトランザクションの中では何もしない。APIが
          途中で commit・rollback した後の begin() も、直列化したトランザクションになる
          （serialize_writes がTrueの間）
        - APIが例外を投げた場合はロールバックする。正常に戻った場合は、まだ開いている
          トランザクションをコミットする（APIがコミット・ロールバック済みなら何もしない。
          コミットせずに書いたAPIの挙動（SQLiteの自動コミット）を変えないため）
        - コミットに失敗した場合もロールバックする
        - 応答（view_json・view_download）の確定は、コミットが成功するまで遅らせる
          （defer_finish）。先に確定すると、ほかの接続の読み取りでSQLiteのCOMMITが
          待たされて失敗した場合などに、コミットされていない変更を成功として返してしまう。
          失敗した場合は書き込んだ応答を破棄し、例外を送出する（proc_accessがエラーを応答する）
        - 読み取り（GET）は直列化しない（ほかのリクエストを待たせないため）
        """
        method = getattr(page, self.API_METHODS[mode])
        if mode == "get":
            method(self, args)
            return
        db = self.ctrl_db["db_control"]
        db.serialize_writes = True
        self.defer_finish = True
        try:
            db.begin()
            try:
                method(self, args)
                db.commit()
            except web.Finish:
                # APIが応答を書いて打ち切った場合（web.Finish）は、書いた応答を残す
                # （Tornadoがfinishする）。変更はコミットしない
                try:
                    db.rollback()
                except BaseException:
                    # ロールバックに失敗した場合はエラーを応答するため、書いた応答を破棄する
                    self.clear()
                    raise
                raise
            except BaseException:
                # コミットされていない処理の応答を返さないよう破棄する。
                # ロールバック自体が失敗した場合（接続断等）も破棄する（破棄しないと、
                # proc_accessが書きかけの応答の後ろにエラーのJSONを追記してしまう）
                try:
                    db.rollback()
                finally:
                    self.clear()
                raise
        finally:
            db.serialize_writes = False
            self.defer_finish = False
        if not self._finished:
            self.finish()

    def proc_access(self, mode, args):
        try:
            if self.is_error:
                self.set_http_status(500, "CE-9999", [self.INTERNAL_ERROR_DETAIL])
                self.view_json({
                    "error": {
                        "code": "CE-9999",
                        "message": self.get_message("CE-9999", [self.INTERNAL_ERROR_DETAIL])
                    }
                })
                return
            operation = self.get_operation()
            if len(operation) > 0:
                self.prm_req["page"] = operation[0]["return_operation"]
            key = self.request.path
            for page in self.pages:
                if self.prm_cmn.get("maintenance", "0") == "1" and not page.maintenance_use:
                    if not page.check_login() or not self.prm_cmn.get("admin", False):
                        self.set_http_status(503)
                        self.view_json({
                            "error": {
                                "code": "CE-0004",
                                "message": self.get_message("CE-0004")
                            }
                        })
                        return
                if page.need_login:
                    if not self.check_login():
                        self.set_http_status(401)
                        self.view_json({
                            "error": {
                                "code": "CE-0002",
                                "message": self.get_message("CE-0002")
                            }
                        })
                        return
                    self.append_access_hist()
                if page.need_auth != "" and not page.need_auth in self.prm_cmn["account_auth"]:
                    self.set_http_status(401)
                    self.view_json({
                        "error": {
                            "code": "CE-0002",
                            "message": self.get_message("CE-0002")
                        }
                    })
                    return
                self.call_api(page, mode, args)
                return
            self.set_http_status(404, "CE-0006", [key])
            self.view_json({
                "error": {
                    "code": "CE-0006",
                    "message": self.get_message("CE-0006", [key])
                }
            })
        except web.HTTPError as e:
            raise e
        except web.Finish:
            # APIが応答を書いて打ち切った場合（web.Finish）は、書いた応答のままTornadoに
            # 確定させる（下のExceptionで捕まえると500のエラー応答を追記してしまうため）
            raise
        except ExceptCommon as e:
            self.set_http_status(500, e.msg_id, e.param)
            self.view_json({
                "error": {
                    "code": e.msg_id,
                    "message": self.get_message(e.msg_id, e.param)
                }
            })
        except Exception as e:
            self.append_log(self.prm_cmn.get("account_id", "Non Login"), "alert")
            print(e)
            print(traceback.format_exc())
            # 例外の内容（メッセージ・スタックトレース）はサーバー側のログにのみ残し、
            # レスポンスには含めない。実装の内部情報（DBのカラム名・SQL・ファイルパス等）を
            # 未認証の相手も含めた呼び出し元へ漏らさないため（prepareの異常時と同じ文言）。
            self.set_http_status(500, "CE-9999", [self.INTERNAL_ERROR_DETAIL])
            self.view_json({
                "error": {
                    "code": "CE-9999",
                    "message": self.get_message("CE-9999", [self.INTERNAL_ERROR_DETAIL])
                }
            })
