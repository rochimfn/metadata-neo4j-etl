import os
from pathlib import Path
from dotenv import load_dotenv


def setup():
    env_path = Path(os.path.dirname(os.path.realpath(__file__))) / '.env'
    load_dotenv(dotenv_path=env_path)
