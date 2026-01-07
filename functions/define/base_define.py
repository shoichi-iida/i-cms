# -*- coding: utf-8 -*-

########################################################################
# モジュールのインポート
########################################################################
import os
import json
import xml.dom.minidom as md
import xml.etree.ElementTree as ET
from collections import OrderedDict

class BaseDefine:
	"""
	外部定義クラス
	"""
	def __init__(self, path):
		"""
		コンストラクタ

		Parameters
		----------
		path : string
			外部定義読込パス
		"""
		if path.endswith(".xml"):
			self.dict = self.load_xml(path)
		elif path.endswith(".json"):
			self.dict = self.load_json(path)

	def __del__(self):
		"""
		デストラクタ
		"""
		self.dict = None

	def load_xml(self, path):
		"""
		外部定義読込処理(XML)

		Parameters
		----------
		path : string
			外部定義読込パス
		"""
		dict = OrderedDict()
		if not os.path.exists(path):
			return dict
		doc = ET.parse(path)
		root = doc.getroot()
		for child in root:
			param = OrderedDict()
			for att in child.attrib.items():
				param[att[0]] = att[1]
			self.__load_child(child, param)
			dict[param["id"]] = param
		return dict

	def load_json(self, path):
		"""
		外部定義読込処理(JSON)

		Parameters
		----------
		path : string
			外部定義読込パス
		"""
		dict = OrderedDict()
		if not os.path.exists(path):
			return dict
		lst = []
		with open(path, mode="rt", encoding="utf-8") as f:
			lst = json.load(f)
		for child in lst:
			dict[child["id"]] = child
		return dict

	def __load_child(self, root, dict):
		for child in root:
			param = {}
			for att in child.attrib.items():
				param[att[0]] = att[1]
			self.__load_child(child, param)
			if child.tag in dict.keys():
				dict[child.tag].append(param)
			else:
				dict[child.tag] = [param]

	def save_xml(self, path, indent=False):
		"""
		外部定義書込処理(XML)

		Parameters
		----------
		path : string
			外部定義書込パス
		"""
		root = ET.Element("define")
		for key in self.dict.keys():
			child = ET.Element("data")
			self.__save_child(child, self.dict[key])
			root.append(child)
		if indent:
			doc = md.parseString(ET.tostring(root, "utf-8"))
			with open(path, "w") as f:
				doc.writexml(f, encoding="utf-8", newl="\n", indent="", addindent="\t")
				f.close()
		else:
			doc = ET.ElementTree(root)
			doc.write(path, "utf-8", True)

	def save_json(self, path):
		"""
		外部定義書込処理(JSON)

		Parameters
		----------
		path : string
			外部定義書込パス
		"""
		with open(path, mode="wt", encoding="utf-8") as f:
			json.dump(list(self.dict.values()), f, ensure_ascii=False, indent="\t")

	def __save_child(self, root, data):
		for key in data.keys():
			obj = data[key]
			if isinstance(obj, list):
				for r in obj:
					child = ET.Element(key)
					self.__save_child(child, r)
					root.append(child)
			else:
				root.attrib[key] = data[key]
