import pandas as pd
import requests
import os
from dotenv import load_dotenv

"""
1. gitflow command 정리&연습 
2. .env 파일에 url, id, password 업데이트
3. 기네스 크롤링 코드(source.py)이름으로 git에 업로드 하기 
   - url, id, password 지워서
   - feature/source_upload에 업로드
   - develop에 merge & push
   - master에 merge & push
"""
def handler(event, context):
    load_dotenv()

if __name__ =="__main__":
    handler({},{})