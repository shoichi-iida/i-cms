# -*- coding: utf-8 -*-
from functions.common.util_check import UtilCheck


class UtilEncrypt():
	"""
	暗号化・復号化ユーティリティクラス
	
	Notes
	-----
	本クラスの関数は静的関数となるため、クラスオブジェクトの生成を行わずに使用すること。
	"""

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
