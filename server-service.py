#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(__file__))

import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import subprocess

from glob import glob
from importlib import import_module
from tornado import ioloop, httpserver, web, options
from tornado.log import app_log
from tornado.options import options

class CmsService(win32serviceutil.ServiceFramework):
	_svc_name_ = "i-cms-service"
	_svc_display_name_ = "i-CMS service"

	def __init__(self, args):
		win32serviceutil.ServiceFramework.__init__(self, args)
		self.popn = None

	# サービス停止
	def SvcStop(self):
		self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
		self.popn.terminate()
		self.ReportServiceStatus(win32service.SERVICE_STOPPED)

	# サービス開始
	def SvcDoRun(self):
		self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
		# 基準パス設定
		base_path = os.path.dirname(__file__)
		args = [f"python", f"{base_path}/server.py"]
		self.popn = subprocess.Popen(args, cwd=base_path)
		self.ReportServiceStatus(win32service.SERVICE_RUNNING)
		sstdout, sstderr = self.popn.communicate()

if __name__ == "__main__":
	win32serviceutil.HandleCommandLine(CmsService)
