import stat
import paramiko
from datetime import datetime, timedelta

class UtilSsh:
	"""
	SSH接続ユーティリティクラス
	"""
	
	def __init__(self, ip, user, pwd, port=22):
		self.server_ip = ip
		self.server_port = port
		self.server_user = user
		self.server_pwd = pwd
		self.client = paramiko.SSHClient()
		self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		self.sftp_connection = None
	
	def __del__(self):
		self.close()
	
	def connect(self):
		try:
			self.client.connect(self.server_ip, username=self.server_user, password=self.server_pwd, port=self.server_port, timeout=5.0)
			(stdin, stdout, stderr) = self.client.exec_command("pwd")
			self.work_dir = stdout.readlines()[0].strip()
			self.sftp_connection = self.client.open_sftp()
			return True
		except:
			return False
	
	def close(self):
		try:
			if self.sftp_connection is not None:
				self.sftp_connection.close()
			if self.client is not None:
				self.client.close()
				self.client = None
		except:
			pass
	
	def exsists(self, path):
		try:
			self.sftp_connection.stat(path)
			return True
		except:
			return False

	def mkdir(self, path, mode=0o511):
		try:
			self.sftp_connection.mkdir(path, mode)
			return True
		except:
			return False

	def chmod(self, path, mode=0o511):
		try:
			self.sftp_connection.chmod(path, mode)
			return True
		except:
			return False

	def change_dir(self, path):
		(stdin, stdout, stderr) = self.client.exec_command(f"cd {path}")
		lst_err = stderr.readlines()
		if len(lst_err) > 0:
			for err in lst_err:
				print(err)
			return False
		self.work_dir = path
		return True
	
	def command(self, cmd):
		stdin, stdout, stderr = self.client.exec_command(f"cd {self.work_dir};{cmd}")
		stdout._set_mode('b')
		stderr._set_mode('b')
		result = {
			"stdout": stdout.readlines(),
			"berr": stdout.read(),
			"stderr": stderr.readlines()
		}
		for std in ["stdout", "stderr"]:
			lst = []
			for line in result[std]:
				for code in ["utf-8", "utf-8_sig", "shift_jis", "euc_jp"]:
					try:
						lst.extend(self._decode_command(line, code))
						break
					except UnicodeDecodeError:
						lst.append(f"{std}のデコードに失敗しました")
			result[std] = lst
		return result
	
	def _decode_command(self, result, code):
		return list(filter(None, result.decode(code, errors="backslashreplace").replace("\r\n", "\n").replace("\r", "\n").split("\n")))

	def get_files(self, path=None):
		if path is None:
			path = self.work_dir
		self.sftp_connection.chdir(path)
		lst = self.sftp_connection.listdir_attr()
		result = []
		for item in lst:
			if item.st_mode & stat.S_IFREG:
				result.append(item.filename)
		return result

	def get_dirs(self, path=None, encoding="utf8"):
		if path is None:
			path = self.work_dir
		self.sftp_connection.chdir(path)
		lst = self.sftp_connection.listdir_attr(encoding=encoding)
		result = []
		for item in lst:
			tmp = {
				"name": item.filename,
				"link": False,
				"size": item.st_size,
				"update_date": datetime.fromtimestamp(item.st_mtime)
			}
			if item.st_mode & stat.S_IFDIR:
				tmp["category"] = "1"
			if item.st_mode & stat.S_IFREG:
				tmp["category"] = "2"
			if stat.S_ISLNK(item.st_mode):
				tmp["link"] = True
				tmp["link_path"] = self.sftp_connection.readlink(f"{path}/{item.filename}")
			result.append(tmp)
		return result

	def get_update_date(self, remote_file):
		utime = self.sftp_connection.stat(remote_file).st_mtime
		return datetime.fromtimestamp(utime)

	def sftp_put(self, local_file, remote_file):
		self.sftp_connection.put(local_file, remote_file)

	def sftp_putfo(self, local_file, remote_file):
		self.sftp_connection.putfo(local_file, remote_file)

	def sftp_get(self, remote_file, local_file):
		self.sftp_connection.get(remote_file, local_file)
