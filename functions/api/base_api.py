# -*- coding: utf-8 -*-

import bisect
import re
from datetime import date, datetime, timedelta, timezone


class BaseApi():
    # DBの整数型（SQLiteのINTEGER・MySQLのBIGINT。符号付き64bit）の最大値。
    # これを超える値はバインド時にエラーとなるため、IDとしては存在しないものとして扱う
    DB_INT_MAX = 2 ** 63 - 1

    # IN句へ一度に展開するIDの件数。リクエストで件数に上限の無いIDを照会する場合、
    # DBのバインド変数の上限（SQLiteのSQLITE_MAX_VARIABLE_NUMBER。古い版では999）を
    # 超えないように、この件数ずつに分けて照会する
    DB_IN_CHUNK_SIZE = 500

    # URLパスのID（/api/v1/projects/{id} 等の{id}）を受けるパターン
    #
    # ([0-9]+)で定義すると数値でないID（abc・1.5等）がどのURLにも一致せず、
    # ApiHandlerに届く前にTornado既定のHTML 404が返る（error.codeを持たない）。
    # パターンは広く受け、IDの形式はApi側でparse_path_idにより判定する。
    # 形式不正のIDは存在しないIDと同じエラー（*_NOT_FOUND等）で返す。
    PATH_ID_PATTERN = "([^/]+)"

    def __init__(self):
        """
        コンストラクタ
        """
        self.handler_url = "/"
        self.maintenance_use = False
        self.need_login = True
        self.need_auth = ""

    def get_account_data(self, handler):
        """
        ログイン中アカウントの情報（tbl_accountの行）を返す。未設定の場合は空dictを返す。
        """
        return handler.prm_cmn.get("account_data") or {}

    def is_admin_account(self, handler):
        """
        ログイン中アカウントが管理者（tbl_account.admin=True）かを返す。
        SQLiteは0/1で返すためboolで評価する。
        """
        return bool(self.get_account_data(handler).get("admin"))

    def get_request_cache(self, handler):
        """
        1リクエスト内だけ有効な読み取りキャッシュを返す。

        認可・データスコープの解決は1リクエストで何度も呼ばれる
        （check_permission、バリデーションごとのスコープ解決など）が、
        参照するのはいずれもリクエスト内で変化しないマスタ・紐づけである。
        ハンドラはリクエストごとに生成されるため、ハンドラへ載せる。

        **読み取り専用のデータにのみ使用すること。** 同一リクエスト内で
        ロール紐づけ・パーミッション紐づけ・組織を更新したあとに
        再判定する処理を書く場合は、キャッシュを使わずに取得すること。

        ログイン中アカウントが変わった場合はキャッシュを捨てる
        （テストで1つのハンドラを使い回すケースへの保険）。
        """
        account_id = self.get_account_data(handler).get("id")
        cache = getattr(handler, "_authz_cache", None)
        if cache is None or cache.get("account_id") != account_id:
            cache = {"account_id": account_id}
            handler._authz_cache = cache
        return cache

    def get_body_value(self, handler, key, default):
        """
        リクエストボディ（JSON）の値を返す。ボディがオブジェクト（dict）でない場合
        （JSONの null・配列・数値など）や、キーが無い場合は default を返す。

        オブジェクトでないJSONのボディは ApiHandler.load_json_body が400 VALIDATION_ERROR で
        拒否するため、HTTP経由では prm_body は常に dict になる。削除API の updatedAt
        （楽観ロック）は、ハンドラ層を通らずに呼ばれた場合（単体テスト等）の防御として
        本メソッドで読む。
        """
        body = handler.prm_body
        if not isinstance(body, dict):
            return default
        return body.get(key, default)

    def is_updated_at_unchanged(self, handler, table, row_id, updated_at):
        """
        楽観ロック：行の更新日時（updated_at）が、リクエストのupdatedAtと一致するかを返す。
        行が無い場合はFalse。

        削除画面は表示した時点の更新日時を送る。表示の後に別のユーザーが
        修正した行を、修正前の内容を見たまま削除しないようにする。
        delete_validation（ほかのチェックの後）と、削除のトランザクション内（取得し直した
        行）から呼び出す。検証と削除の間に、別のリクエストが修正した場合への備え
        （更新系のリクエストは ApiHandler.call_api が直列化する）。
        """
        rows = handler.ctrl_db["db_control"].select(table, dict_select={"id": row_id})
        return len(rows) > 0 and str(rows[0]["updated_at"]) == str(updated_at)

    def parse_path_id(self, value):
        """
        URLパスのID（PATH_ID_PATTERNで受けた文字列）を正規化する。

        正の整数（半角数字のみ。先頭ゼロは許容し、"007"は7）であれば10進数の文字列
        （"7"）を、そうでなければNoneを返す。0・負数・小数・空白や全角を含む値・
        DB_INT_MAXを超える値（DBの整数型に収まらず、バインドすると500になる）はNone。
        呼び出し側は、存在しないIDを指定したときと同じエラー（*_NOT_FOUND等）を返す。
        レスポンスでURLパスの値を文字列のまま返すAPIがあるため、
        型は文字列のままとする。
        """
        if not isinstance(value, str) or not value.isascii() or not value.isdigit():
            return None
        # 桁数の多い文字列はint()が上限（既定4300桁）でValueErrorとなるため、
        # 先頭ゼロを除いた桁数で弾いてから変換する（DB_INT_MAXの桁数を超えるものは範囲外）
        digits = value.lstrip("0")
        if len(digits) == 0 or len(digits) > len(str(self.DB_INT_MAX)):
            return None
        parsed = int(digits)
        if parsed > self.DB_INT_MAX:
            return None
        return str(parsed)

    def parse_optional_int(self, value):
        """
        任意指定の数値項目（ID・階層レベル等）をintへ正規化する。

        未指定・空文字・Noneは「指定なし」としてNoneを返す。
        整数として解釈できない値はValueErrorを送出するため、呼び出し側で
        400 VALIDATION_ERRORとして扱う。

        bool・floatは弾く。Pythonのint()はJSONのtrueを1、1.9を1として
        例外なく受け入れるため、そのままでは意図しないIDへ丸め込まれる。
        小数を含む文字列（"1.9"）もint()がValueErrorを送出するため弾かれる。
        """
        if value is None or value == "":
            return None
        # boolはintのサブクラスのため、int判定より先に弾く
        if isinstance(value, bool):
            raise ValueError("bool is not a valid integer value")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            raise ValueError("float is not a valid integer value")
        return int(str(value).strip())

    def exceeds_max_length(self, value, max_length):
        """
        文字列項目が最大長を超えるかを返す。

        上限ちょうどは受理し（False）、超えた場合にTrueを返す。
        呼び出し側は保存する値（strip()する項目はstrip()の後）を渡す。

        文字数は画面と同じく**UTF-16のコードユニット数**で数える。
        画面のzod・maxlengthはJavaScriptのString.length（UTF-16のコードユニット数）
        で数えるが、Pythonのlen()はコードポイント数を返すため、絵文字など
        U+10000以降の文字（サロゲートペア）を画面は2文字、len()は1文字と数える。
        len()で判定すると画面の上限を超える値がAPI経由で入り、修正画面で
        そのまま保存し直せなくなる（zodが弾く）ため、画面の数え方に合わせる。

        encode("utf-16-le")で数えないのは、JSONの"\\ud800"のような単独の
        サロゲートを含む文字列がUnicodeEncodeError（500）になるため。
        単独のサロゲートはJavaScriptと同じく1コードユニットとして数える
        （単独のサロゲートを含む値は、上限以内でもcontains_lone_surrogateで弾く）。

        文字列でない値（未指定のNone等）は判定の対象外としてFalseを返す
        （必須・型のチェックは呼び出し側で行う）。
        """
        if not isinstance(value, str):
            return False
        # UTF-16のコードユニット数はコードポイント数以上のため、
        # len()で超えていれば数えるまでもない（巨大な文字列を走査しない）
        if len(value) > max_length:
            return True
        units = len(value) + sum(1 for ch in value if ord(ch) > 0xFFFF)
        return units > max_length

    def contains_lone_surrogate(self, *values):
        """
        いずれかの文字列項目が単独のサロゲート（U+D800〜U+DFFF）を含むかを返す。

        JSONの"\\ud800"のような対になっていないサロゲートのエスケープは、
        json.loadsで単独のサロゲートを含むstrになる。UTF-8へエンコードできないため、
        DBへの登録（sqlite3）やパスワードのハッシュ化（bcrypt）で
        UnicodeEncodeErrorになり500を返していた。保存する前にこの判定で弾き、
        400 VALIDATION_ERRORを返す。DBの検索（sqlite3）も同じく500になるため、
        AUTH001はuserIdをDBの検索の前に判定し、存在しないユーザーIDと同じ
        401 AUTH_FAILEDを返す。

        正しいサロゲートペア（"\\ud83d\\ude00"）はjson.loadsで1つのコードポイント
        （U+1F600）に戻るため、strにサロゲートが残るのは対になっていない場合のみ。

        文字列でない値（未指定のNone等）は判定の対象外とする。
        """
        for value in values:
            if isinstance(value, str) and any(0xD800 <= ord(ch) <= 0xDFFF for ch in value):
                return True
        return False

    # 制御文字のチェック。1行の項目と複数行の項目で受け付けない文字が違う。
    # 日本語・記号・全角英数字・絵文字は制限しない（DBはSQLite（UTF-8）で4バイトの文字も保存できる）
    #
    # 1行の項目: 制御文字（U+0000〜U+001F・U+007F〜U+009F）と行区切り（U+2028）・
    #   段落区切り（U+2029）。改行・タブも不可（画面の1行の入力欄に入らない文字のため）
    # 複数行の項目: 改行（\n・\r）とタブ（\t）以外の制御文字
    MULTILINE_ALLOWED_CONTROL_CHARS = ("\n", "\r", "\t")

    @staticmethod
    def is_control_char(ch):
        """制御文字（U+0000〜U+001F・U+007F〜U+009F）かを返す"""
        code = ord(ch)
        return code <= 0x1F or 0x7F <= code <= 0x9F

    def contains_control_char(self, *values):
        """
        いずれかの1行の文字列項目が、受け付けない文字を含むかを返す。

        制御文字（U+0000〜U+001F・U+007F〜U+009F。改行・タブを含む）と、
        行区切り（U+2028）・段落区切り（U+2029）。1行の項目は前後の空白を除いた
        後（strip）で判定するため、前後の改行・タブは除かれて受け付ける。
        文字列でない値（未指定のNone等）は判定の対象外とする。
        """
        for value in values:
            if isinstance(value, str) and any(
                    self.is_control_char(ch) or ch in ("\u2028", "\u2029") for ch in value):
                return True
        return False

    def contains_multiline_control_char(self, *values):
        """
        いずれかの複数行の文字列項目が、改行（\n・\r）・タブ（\t）以外の制御文字を
        含むかを返す（issue #376）。行区切り（U+2028）・段落区切り（U+2029）は
        制御文字ではないため受け付ける。文字列でない値（None等）は判定の対象外とする。
        """
        for value in values:
            if isinstance(value, str) and any(
                    self.is_control_char(ch) and ch not in self.MULTILINE_ALLOWED_CONTROL_CHARS
                    for ch in value):
                return True
        return False

    def get_request_path(self, handler):
        """
        エラーメッセージへ埋め込むリクエストパスを返す。

        Tornadoのハンドラでは実際のリクエストパス（request.path）を使い、
        それを持たない場合（単体テストのFakeHandler等）はルート定義の
        handler_urlで代替する。
        """
        request = getattr(handler, "request", None)
        return getattr(request, "path", None) or self.handler_url

    def return_error_not_implemented(self, handler):
        """
        設計に存在しないHTTPメソッドへのアクセスに対する応答。

        Apiクラスが該当メソッドを実装していない場合の既定動作であり、
        ApiHandlerのルート未一致時（404 CE-0006）と同じエラーコード・
        共通エラースキーマ（error.code / error.message）・同じ
        メッセージ付随情報（リクエストパス）で返す。
        """
        handler.return_error_json(404, "CE-0006", [self.get_request_path(handler)])

    def get_response(self, handler, args):
        self.return_error_not_implemented(handler)

    def post_response(self, handler, args):
        self.return_error_not_implemented(handler)

    def put_response(self, handler, args):
        self.return_error_not_implemented(handler)

    def patch_response(self, handler, args):
        self.return_error_not_implemented(handler)

    def delete_response(self, handler, args):
        self.return_error_not_implemented(handler)

    def get_login_failure_reset_values(self):
        """
        ログイン失敗回数・ロック解除時刻をクリアする更新値を返す。

        パスワード再設定の更新値へ加え、同じUPDATE文でクリアする。
        呼び出し側で更新値へ追加・変更できるよう、毎回新しいdictを返す。
        """
        return {
            "login_failed_count": 0,
            "login_locked_until": None
        }

    def is_locked(self, account):
        """
        アカウントがロック中かを返す。

        ロック解除時刻（login_locked_until）が未来の場合のみロック中とする。
        解釈できない値が入っている場合はロックなしとして扱う（ログインを
        恒久的に不能にしないため）。

        ログインの可否と画面へ返すロック状態で同じ判定を
        使い、「画面ではロック中でないのにログインは429」というずれを防ぐ。

        本実装が書き込む値はタイムゾーンなし（naive）だが、設計書の日時形式は
        ISO 8601（+09:00付き）であり、移行や手動更新でタイムゾーン付きの値が
        入り得る。naiveとawareの比較はTypeErrorになるため、解除時刻に合わせて
        現在時刻の基準を作ってから比較する。
        """
        locked_until = account.get("login_locked_until")
        if not locked_until:
            return False
        try:
            locked_until = datetime.fromisoformat(str(locked_until))
        except (TypeError, ValueError):
            return False
        now = datetime.now(locked_until.tzinfo) if locked_until.tzinfo else datetime.now()
        return now < locked_until

    def get_login_lock_account(self, handler, user_id):
        """
        ユーザーIDのログイン失敗回数・ロックを記録する行（ロックの対象）を返す。

        userId かつ is_active=true の行のうち、idの昇順で先頭の行。
        該当する行が無い場合（存在しないユーザーID、削除済みの行しかない
        ユーザーID）はNoneを返す。

        ロックはユーザーID単位であり、ログインの可否は本メソッドの行に対して
        加算・判定・リセットを行う。ロック状態の表示・ロック解除
        も同じ行を参照し、「画面では解除済みなのにログインは429」
        というずれを防ぐ。

        - 削除済みの行は対象にしない。アカウントの削除は論理削除であり、
          ユーザーIDの重複チェックは有効なアカウントのみを
          対象とするため、同じuser_idの行が複数並び得る。削除済みの行を対象に
          すると、ロックが削除済みの行に記録され、有効な行とずれる
          （認証の照合（base_handler.get_account）と同じ絞り込み）
        - 有効な行が複数あることは重複チェックにより通常は起きないが、判定が
          ぶれないようidの昇順で先頭の行を選ぶ（ORDER BYのないSELECTの
          返却順はDBの実装に依存するため、取得した先頭の行にはしない）
        """
        rows = handler.ctrl_db["db_control"].select("tbl_account", dict_select={
            "user_id": user_id,
            "is_active": True
        })
        if len(rows) == 0:
            return None
        return min(rows, key=lambda row: row["id"])

    def normalize_account_id(self, account_id):
        """
        アカウントID（tbl_account.id）を数値へ正規化する。

        URLから受け取ったIDは文字列、DBから取得したIDは数値のため、そろえて比較・更新する。
        TinyDBの条件は型まで一致する必要があり（"0" と 0 は一致しない）、文字列のままでは
        初期化（InitializeData）で数値のIDを保存した行を更新できないため、数値にする。
        数値として解釈できない値はそのまま返す。
        """
        try:
            return int(account_id)
        except (TypeError, ValueError):
            return account_id

    def reset_login_failure(self, handler, *account_ids):
        """
        ログイン失敗回数とロック解除時刻をクリアする（独立したトランザクション）。

        ログイン成功・ロック満了時と管理者によるロック解除で
        使う。ロック関連の値だけを変える更新は業務上の更新ではないため、
        updated_at / updated_byは変更しない（楽観ロックの
        判定に影響させない）。

        複数のアカウントIDを渡すと、同じトランザクションでまとめてクリアする
        （指定された行とロックの対象の行が異なる場合）。
        同じIDは1回だけ更新する（URLの文字列とDBの数値が混在してもよい）。

        本メソッドは内部でbegin/commitする。他の更新と同じトランザクションで
        クリアしたい場合（パスワード再設定）は、
        get_login_failure_reset_valuesを更新値へ加えること。
        """
        db = handler.ctrl_db["db_control"]
        try:
            db.begin()
            for account_id in dict.fromkeys(self.normalize_account_id(i) for i in account_ids):
                db.update("tbl_account", self.get_login_failure_reset_values(),
                          {"id": account_id})
            db.commit()
        except Exception:
            # 元のスタックトレースを保つため raise e ではなく raise で再送出する
            db.rollback()
            raise
