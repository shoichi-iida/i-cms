# -*- coding: utf-8 -*-
import bcrypt
from tornado.options import options

from functions.common.util_check import UtilCheck


class UtilEncrypt():
	"""
	暗号化・復号化ユーティリティクラス

	Notes
	-----
	本クラスの関数は静的関数となるため、クラスオブジェクトの生成を行わずに使用すること。

	パスワードの保存にはhash_password（bcrypt）を使用し、照合はverify_passwordで行う。
	encrypt_xor / decrypt_xorは可逆暗号のためパスワードの保存には使用しないこと
	（既存データのbcryptへの移行（migrate_password_to_bcrypt.py）でのみ使用する）。
	"""

	# bcryptハッシュのプレフィクス。ハッシュ済みかの判定に使用する
	BCRYPT_PREFIXES = ("$2a$", "$2b$", "$2y$")

	# bcryptが受け付けるパスワードの最大バイト数（アルゴリズム上の制約）
	BCRYPT_MAX_BYTES = 72

	# bcryptのコストファクタ（設定 bcrypt_rounds 未指定時の既定値）
	BCRYPT_DEFAULT_ROUNDS = 12

	# bcryptが受け付けるコストファクタの範囲
	BCRYPT_MIN_ROUNDS = 4
	BCRYPT_MAX_ROUNDS = 31

	@staticmethod
	def hash_password(plain):
		"""
		パスワードのハッシュ化処理（bcrypt）

		Parameters
		----------
		plain : string
			平文パスワード

		Returns
		-------
		string
			bcryptハッシュ（60文字）。plainが空の場合はplainをそのまま返す

		Notes
		-----
		bcryptは72バイトを超える入力を受け付けないため、UTF-8エンコード後に
		72バイトへ切り詰める。パスワード規定は「8文字以上50文字以下」で文字種を
		バイト数で制限していないため、多バイト文字のみのパスワードは72バイトを
		超えうる（例: 全角50文字＝150バイト）。
		verify_passwordでも同じ切り詰めを行うため、照合結果は一致する。
		"""
		if UtilCheck.is_empty(plain):
			return plain
		return bcrypt.hashpw(
			UtilEncrypt.truncate_password(plain),
			bcrypt.gensalt(rounds=UtilEncrypt.get_bcrypt_rounds())
		).decode("ascii")

	@staticmethod
	def get_bcrypt_rounds():
		"""
		bcryptのコストファクタを返す

		Returns
		-------
		int
			設定（bcrypt_rounds）の値。未定義・不正値の場合はBCRYPT_DEFAULT_ROUNDS

		Notes
		-----
		コストファクタを1増やすとハッシュ計算時間が2倍になる。既定値（12）は
		1回あたり数百ミリ秒かかるため、テストでは低い値（4）を設定して実行時間を
		抑えている（tests/api_test_helper.py・tests_integration/it_helper.py）。
		本番設定を下げるとブルートフォース耐性が落ちるため、server.iniの値は
		変更しないこと。
		"""
		rounds = UtilEncrypt.BCRYPT_DEFAULT_ROUNDS
		if "bcrypt_rounds" in options:
			try:
				rounds = int(options.bcrypt_rounds)
			except (TypeError, ValueError):
				rounds = UtilEncrypt.BCRYPT_DEFAULT_ROUNDS
		return max(
			UtilEncrypt.BCRYPT_MIN_ROUNDS,
			min(UtilEncrypt.BCRYPT_MAX_ROUNDS, rounds)
		)

	@staticmethod
	def verify_password(plain, hashed):
		"""
		パスワードの照合処理（bcrypt）

		Parameters
		----------
		plain : string
			平文パスワード
		hashed : string
			保存されているbcryptハッシュ

		Returns
		-------
		bool
			一致する場合True。未ハッシュ（XOR暗号のまま）の場合はFalseを返す
		"""
		if UtilCheck.is_empty(plain) or not UtilEncrypt.is_hashed_password(hashed):
			return False
		try:
			return bcrypt.checkpw(
				UtilEncrypt.truncate_password(plain), str(hashed).encode("ascii")
			)
		except (ValueError, TypeError):
			return False

	@staticmethod
	def is_hashed_password(value):
		"""
		bcryptでハッシュ化済みの値かを返す

		Parameters
		----------
		value : string
			判定対象（tbl_account.passwordの値）

		Returns
		-------
		bool
			bcryptハッシュの場合True
		"""
		if UtilCheck.is_empty(value):
			return False
		return str(value).startswith(UtilEncrypt.BCRYPT_PREFIXES)

	@staticmethod
	def truncate_password(plain):
		"""
		パスワードをbcryptの上限（72バイト）までのbytesへ変換する

		Parameters
		----------
		plain : string
			平文パスワード

		Returns
		-------
		bytes
			UTF-8エンコード後、先頭72バイトまで
		"""
		return str(plain).encode("utf-8")[:UtilEncrypt.BCRYPT_MAX_BYTES]

	@staticmethod
	def encrypt_xor(value, key):
		"""
		暗号化処理

		Parameters
		----------
		value : string
			暗号化対象文字列
		key : string
			暗号化キー
		
		Returns
		-------
		string
			暗号化文字列
		"""
		if UtilCheck.is_empty(value) or UtilCheck.is_empty(key):
			return value
		xor_cd = key
		while len(value) > len(xor_cd):
			xor_cd += key
		return "".join(
			[chr(ord(v) ^ ord(cd)) for (v, cd) in zip(value, xor_cd)]
		).encode().hex()

	@staticmethod
	def decrypt_xor(value, key):
		"""
		復号化処理

		Parameters
		----------
		value : string
			復号化対象文字列
		key : string
			復号化キー
		
		Returns
		-------
		string
			復号化文字列
		"""
		if UtilCheck.is_empty(value) or UtilCheck.is_empty(key):
			return value
		crypt = bytes.fromhex(value).decode()
		xor_cd = key
		while len(crypt) > len(xor_cd):
			xor_cd += key
		return "".join(
			[chr(ord(v) ^ ord(cd)) for (v, cd) in zip(crypt, xor_cd)]
		)
