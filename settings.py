import os
import sys
from datetime import datetime
from pathlib import Path

def eprint(text):
    print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}][ERR] {text}', file=sys.stderr)

def oprint(text):
    print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}][OUT] {text}', file=sys.stdout)

def check():
	if not os.path.exists(Path(os.path.dirname(os.path.realpath(__file__))) / '.env'):
		eprint('Berkas .env tidak ditemukan')
		oprint('Operasi dihentikan')
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