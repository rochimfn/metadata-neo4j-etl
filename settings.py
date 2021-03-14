import os
from pathlib import Path
import sys

def check():
	if not os.path.exists(Path(os.path.dirname(os.path.realpath(__file__))) / '.env'):
		print('Berkas .env tidak ditemukan')
		print('Operasi dihentikan')
		sys.exit(1)

def setup():
	check()
	with open(Path(os.path.dirname(os.path.realpath(__file__))) / '.env') as env:
		for line in env:
			key=line.split('=')[0]
			value=line.split('=')[-1].rstrip("\n")
			os.environ[key]=value

def clean():
	with open(Path(os.path.dirname(os.path.realpath(__file__))) / '.env') as env:
		for line in env:
			os.environ.pop(line.split('=')[0])