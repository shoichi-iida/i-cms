from tornado.options import options
from pymsteams import connectorcard

class UtilTeams:
	"""
	Temas通知ユーティリティクラス
	"""
	def __init__(self, url):
		self.teams = connectorcard(url)
		
	def add_title(self, title):
		self.teams.title(title)
	
	def add_text(self, text):
		self.teams.text(text)
	
	def send(self):
		self.teams.send()
