# -*- coding: utf-8 -*-

import re

class UtilCheck():
	"""
	チェック処理ユーティリティクラス
	
	Notes
	-----
	本クラスの関数は静的関数となるため、クラスオブジェクトの生成を行わずに使用すること。
	"""

	@staticmethod
	def is_empty(value):
		"""
		未入力チェック処理

		Parameters
		----------
		value : string
			チェック対象文字列
		
		Returns
		-------
		bool
			True : 未入力　False:入力あり
		"""
		if value is None or value.strip() == "":
			return True
		return False
	
	@staticmethod
	def is_true(value):
		"""
		真偽値判定処理

		DBの真偽値の列の値を判定する。SQLite・MySQLは0/1、TinyDBは保存した値
		（True/False、"true"/"false"、"1"/"0"等）をそのまま返すため、SQL系のDBの保存時
		（ControlBase.get_value）と同じく、"true" / "1" を真とする。

		Parameters
		----------
		value : any
			判定対象の値

		Returns
		-------
		bool
			True : 真　False : 偽（None・空文字・"0"・"false"等）
		"""
		if isinstance(value, bool):
			return value
		if value is None:
			return False
		return str(value).strip().lower() in ("true", "1")

	@staticmethod
	def is_decimal(value):
		"""
		数字チェック処理(アラビア数字)

		Parameters
		----------
		value : string
			チェック対象文字列
		
		Returns
		-------
		bool
			True : 未入力、或いは数字のみ　False:数字以外の文字列あり
		
		Notes
		-----
		負数や小数点はFalseとなるので注意
		"""
		if value is None or value.strip() == "":
			return True
		return value.isdecimal()

	@staticmethod
	def is_decimal_option(value, option):
		"""
		数字+追加文字チェック処理

		Parameters
		----------
		value : string
			チェック対象文字列
		option : string
			追加で許容する文字
		
		Returns
		-------
		bool
			True : 未入力、或いは数字+追加文字のみ　False:数字+追加文字以外の文字列あり
		"""
		if value is None or value.strip() == "":
			return True
		if re.match("[^0-9" + option + "]", value):
			return False
		return True

	@staticmethod
	def is_digit(value):
		"""
		数字チェック処理(アラビア数字、特殊数字)

		Parameters
		----------
		value : string
			チェック対象文字列
		
		Returns
		-------
		bool
			True : 未入力、或いは数字のみ　False:数字以外の文字列あり
		
		Notes
		-----
		負数や小数点はFalseとなるので注意
		"""
		if value is None or value.strip() == "":
			return True
		return value.isdigit()
	
	@staticmethod
	def is_numeric(value):
		"""
		数字チェック処理(アラビア数字、特殊数字、漢数字)

		Parameters
		----------
		value : string
			チェック対象文字列
		
		Returns
		-------
		bool
			True : 未入力、或いは数字のみ　False:数字以外の文字列あり
		
		Notes
		-----
		負数や小数点はFalseとなるので注意
		"""
		if value is None or value.strip() == "":
			return True
		return value.isnumeric()
	
	@staticmethod
	def is_alpha(value):
		"""
		英字チェック処理

		Parameters
		----------
		value : string
			チェック対象文字列
		
		Returns
		-------
		bool
			True : 未入力、或いは英字のみ　False:英字以外の文字列あり
		"""
		if value is None or value.strip() == "":
			return True
		return value.isalpha()
	
	@staticmethod
	def is_alnum(value):
		"""
		英数字チェック処理

		Parameters
		----------
		value : string
			チェック対象文字列
		
		Returns
		-------
		bool
			True : 未入力、或いは英数字のみ　False:英数字以外の文字列あり
		"""
		if value is None or value.strip() == "":
			return True
		return value.isalnum()
	
	@staticmethod
	def is_alnum_option(value, option):
		"""
		英数字+追加文字チェック処理

		Parameters
		----------
		value : string
			チェック対象文字列
		option : string
			追加で許容する文字
		
		Returns
		-------
		bool
			True : 未入力、或いは英数字+追加文字のみ　False:英数字+追加文字以外の文字列あり
		"""
		if value is None or value.strip() == "":
			return True
		return re.match(f"^[a-zA-Z0-9{option}]+$", value) is not None
	
	@staticmethod
	def is_double_byte(value):
		if value is None or value.strip() == "":
			return True
		return  any(ord(char) > 255 for char in value)
