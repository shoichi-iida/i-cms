# -*- coding: utf-8 -*-
import base64
import hashlib
import bcrypt
from tornado.options import options

from functions.common.util_check import UtilCheck


class UtilEncrypt():
	"""
	暗号化・復号化ユーティリティクラス

	Notes
	-----
	本クラスの関数は静的関数となるため、クラスオブジェクトの生成を行わずに使用すること。

	パスワードの保存にはhash_password（bcrypt-sha256）を使用し、照合はverify_passwordで行う。
	encrypt_xor / decrypt_xorは可逆暗号のためパスワードの保存には使用しないこと
	（既存データのbcryptへの移行（setup/migrate_data.py）でのみ使用する）。
	"""

	# パスワードハッシュの形式（バージョン）を示すプレフィクス。
	# 「SHA-256で事前ハッシュした値をbcryptでハッシュ化する」形式で、本プレフィクスの
	# 後ろにbcryptハッシュ（$2b$...）が続く
	BCRYPT_SHA256_PREFIX = "$bcrypt-sha256$"

	# 事前ハッシュなしのbcryptハッシュのプレフィクス（旧形式）。
	# 旧形式は72バイト以内のパスワードに限り照合する
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
		パスワードのハッシュ化処理（bcrypt-sha256）

		Parameters
		----------
		plain : string
			平文パスワード

		Returns
		-------
		string
			BCRYPT_SHA256_PREFIX + bcryptハッシュ。plainが空の場合はplainをそのまま返す

		Notes
		-----
		bcryptは72バイトを超える入力を受け付けない。パスワード規定は「8文字以上50文字以下」で
		文字種をバイト数で制限していないため、多バイト文字のみのパスワードは72バイトを
		超えうる（例: 全角50文字＝150バイト）。72バイトで切り詰めると末尾だけが異なる
		パスワードが同じハッシュになるため、パスワード全体をSHA-256で事前ハッシュし
		（prehash_password）、その値をbcryptでハッシュ化する。
		"""
		if UtilCheck.is_empty(plain):
			return plain
		return UtilEncrypt.BCRYPT_SHA256_PREFIX + bcrypt.hashpw(
			UtilEncrypt.prehash_password(plain),
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
		抑えている（tests/test_util_encrypt.py）。
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
		パスワードの照合処理（bcrypt-sha256・bcrypt）

		Parameters
		----------
		plain : string
			平文パスワード
		hashed : string
			保存されているパスワードハッシュ

		Returns
		-------
		bool
			一致する場合True。未ハッシュ（XOR暗号のまま）の場合はFalseを返す

		Notes
		-----
		旧形式（事前ハッシュなしのbcrypt）は、72バイトを超えるパスワードでは照合しない
		（切り詰めて照合すると、末尾だけが異なるパスワードでも一致してしまうため）。
		"""
		if UtilCheck.is_empty(plain) or not UtilEncrypt.is_hashed_password(hashed):
			return False
		hashed = str(hashed)
		try:
			if hashed.startswith(UtilEncrypt.BCRYPT_SHA256_PREFIX):
				return bcrypt.checkpw(
					UtilEncrypt.prehash_password(plain),
					hashed.removeprefix(UtilEncrypt.BCRYPT_SHA256_PREFIX).encode("ascii")
				)
			encoded = str(plain).encode("utf-8")
			if len(encoded) > UtilEncrypt.BCRYPT_MAX_BYTES:
				return False
			return bcrypt.checkpw(encoded, hashed.encode("ascii"))
		except (ValueError, TypeError):
			return False

	@staticmethod
	def is_hashed_password(value):
		"""
		ハッシュ化済みの値かを返す

		Parameters
		----------
		value : string
			判定対象（tbl_account.passwordの値）

		Returns
		-------
		bool
			bcrypt-sha256・bcryptハッシュの場合True
		"""
		if UtilCheck.is_empty(value):
			return False
		return str(value).startswith(
			(UtilEncrypt.BCRYPT_SHA256_PREFIX,) + UtilEncrypt.BCRYPT_PREFIXES)

	@staticmethod
	def prehash_password(plain):
		"""
		パスワードをbcryptへ渡す値（SHA-256の事前ハッシュ）へ変換する

		Parameters
		----------
		plain : string
			平文パスワード

		Returns
		-------
		bytes
			UTF-8エンコードしたパスワード全体のSHA-256をBase64にした値（44バイト）

		Notes
		-----
		SHA-256のダイジェストはNULバイトを含みうる（bcryptはNULバイトを受け付けない）ため、
		Base64へ変換してから渡す。長さは常に44バイトでbcryptの上限（72バイト）に収まる。
		"""
		return base64.b64encode(hashlib.sha256(str(plain).encode("utf-8")).digest())


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
