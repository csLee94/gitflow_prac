from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
from html_table_parser import parser_functions
import time
import requests
from dotenv import load_dotenv
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread as gs
import os
import glob
import collections

collections.Callable = collections.abc.Callable
load_dotenv()
start = time.time()  # 시작 시간 저장
today = datetime.today()
breaktime = today - timedelta(days=999)  # 오늘- days=n "크롤일" n일 전 데이터까지 크롤링

payload = {
    'email': os.environ.get("ID"),
    'password': os.environ.get("pw")
}

with requests.Session() as s:
    p = s.post(os.environ.get("login_url"), data=payload)


# 반복, 페이지 설정
def scraping(url):
    data_url = os.environ.get(f'{url}'"_url")
    data_urlfront = str(data_url.split('1')[0])
    data_urlback = str(data_url.split('1')[1])

    n = 0
    df_row = pd.DataFrame()
    while True:

        n += 1
        get_url = data_urlfront + str(n) + data_urlback

        # parsing
        html = s.get(get_url).text
        soup = BeautifulSoup(html, 'lxml')

        # find data
        data = soup.find("table", {"class": "table table-striped table-bordered table-hover"})
        if data_url == os.environ.get("Account_url"):
            data = soup.find_all("table", {"class": "table table-striped table-bordered table-hover"})[-1]

        # creat table from data
        table = parser_functions.make2d(data)
        df = pd.DataFrame(data=table[1:], columns=table[0])

        columns_list = list(df.columns)
        if "사용시간" in columns_list:
            df.rename(columns={"사용시간": 'crawled_on'}, inplace=True)
        if '크롤일' in columns_list:
            df.rename(columns={'크롤일': 'crawled_on'}, inplace=True)
        if "ID(CODE)" in columns_list:
            df.rename(columns={'ID(CODE)': 'ID'}, inplace=True)

        filter_df = df['crawled_on'] != '0000-00-00 00:00:00'
        df = df[filter_df]
        df['crawled_on'] = pd.to_datetime(df['crawled_on'])

        try:
            df_4_break = (df.loc[0, 'crawled_on'])
        except KeyError:
            print("Scrap break at", get_url)
            break

        if df_4_break < breaktime:
            break

        df.drop(df[df['crawled_on'] < breaktime].index, inplace=True)
        df_row = pd.concat([df_row, df])

    if f'{url}' == 'Channel':
        # Filter_SNS
        drop_analytics = df_row.loc[:, 'SNS'] != 'analytics'
        df = df_row[drop_analytics]
        filter_insta = df.loc[:, 'SNS'] != 'youtube'
        df[filter_insta].to_csv(f"C:\\test\\csv\\Row_csv\\InstaChannel_Row.csv", index=False, encoding='utf-8-sig')
        filter_youtube = df.loc[:, 'SNS'] != 'instagram'
        df[filter_youtube].to_csv(f"C:\\test\\csv\\Row_csv\\YoutubeChannel_Row.csv", index=False, encoding='utf-8-sig')
    else:
        df_row.to_csv(f"C:\\test\\csv\\Row_csv\\{url}_Row.csv", index=False, encoding='utf-8-sig')
    print("Done "f'{url}'"\ntime :", time.time() - start, "\n")  # running_time


def read_json_file():
    os.chdir(r'C:\test\JsonFile')
    extension = 'json'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    foldernames = os.listdir(r'C:\test\JsonFile')
    today = str(date.today())

    def save_to_csv(channel):
        file_lst = list(filter(lambda x: x.startswith(f'{channel}'), all_filenames))
        fdf = pd.DataFrame()
        for file_name in file_lst:
            tdf = pd.read_json(file_name)
            fdf = pd.concat([fdf, tdf]).drop_duplicates()

        fdf.to_csv(f"C:/test/csv/Row_csv/Json_{channel}_Row.csv", encoding='utf-8-sig', index=False)

    if foldernames.count(today) >= 1:
        os.chdir(f'C:/test/JsonFile/{today}')
        extension = 'json'
        all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

        for channel in ['instagram', 'youtube']:
            save_to_csv(channel)
        print("succeed to save\n")

    else:
        print("doesn't exist today folder\n")


def preprocessing():
    df_sample = pd.DataFrame()

    for Row in ["Account", "InstaChannel", "Tag", "YoutubeChannel", "Json_instagram", "Json_youtube"]:
        if f'{Row}' in ["Account", "InstaChannel", "Tag", "YoutubeChannel"]:
            # df = pd.read_csv(f'C:\\test\\csv\\Row_csv\\{Row}_Row.csv', low_memory=False)
            # df_2_merge = pd.read_csv(f'C:\\test\\csv\\Merge_csv\\{Row}_Merge.csv', low_memory=False)

            if os.path.isfile(f'C:\\test\\csv\\Merge_csv\\{Row}_Merge.csv'):
                df = pd.read_csv(f'C:\\test\\csv\\Row_csv\\{Row}_Row.csv', low_memory=False)
                df_2_merge = pd.read_csv(f'C:\\test\\csv\\Merge_csv\\{Row}_Merge.csv', low_memory=False)
            else:
                df = pd.read_csv(f'C:\\test\\csv\\Row_csv\\{Row}_Row.csv', low_memory=False)
                df_2_merge = pd.read_csv(f'C:\\test\\csv\\Row_csv\\{Row}_Row.csv', low_memory=False)

            if f'{Row}' in ["Account"]:
                df = pd.concat([df, df_2_merge])
                df['crawled_on'] = pd.to_datetime(df['crawled_on']).dt.date

                df = df.drop_duplicates(subset=["이름", "crawled_on"], keep="last").reset_index(drop=True)
            elif f'{Row}' in ["Tag"]:
                df = pd.concat([df, df_2_merge])
                df['crawled_on'] = pd.to_datetime(df['crawled_on']).dt.date

                df = df.drop_duplicates(subset=["태그명", "crawled_on"], keep="last").reset_index(drop=True)

            else:
                df = pd.concat([df, df_2_merge])

                df['crawled_on'] = pd.to_datetime(df['crawled_on']).dt.date
                df = df.drop_duplicates(subset=["ID", "crawled_on"], keep="last").reset_index(drop=True)

            df.to_csv(f'C:\\test\\csv\\Merge_csv\\{Row}_Merge.csv', index=False, encoding='utf-8-sig')
        else:
            df = pd.read_csv(f'C:\\test\\csv\\Row_csv\\{Row}_Row.csv', low_memory=False)

        df['crawled_on'] = pd.to_datetime(df['crawled_on']).dt.date

        df_processing = df['crawled_on'].value_counts().reset_index()
        df_processing.columns = ["crawled_on", f'{Row}'"_crawl_count"]

        df_sample = pd.concat([df_processing, df_sample])

    df_out = df_sample.groupby('crawled_on').sum().reset_index().sort_values(by="crawled_on", ignore_index=True,
                                                                             ascending=False)

    new_date_range = pd.date_range(start=df_out["crawled_on"].iloc[-1], end=date.today(), freq="D")

    df_out.set_index(df_out['crawled_on'], inplace=True)
    df_out = df_out.reindex(new_date_range, fill_value=0).reset_index()
    df_out["crawled_on"] = df_out["index"]
    df_out.drop(columns='crawled_on', inplace=True)
    df_out.rename(columns={'index': 'crawled_on'}, inplace=True)

    df_out.to_csv(r'C:\test\csv\G+T_Result.csv', index=False, encoding='utf-8-sig')
    print("Done preprocessing\ntime :", time.time() - start, "\n")  # running_time


def loading_on_spreadsheet():
    df = pd.read_csv(r'C:\test\csv\G+T_Result.csv')

    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        r'C:\test\JsonFile\wise-vim-347706-d410a3b63092.json',
        scope)

    gc = gs.authorize(credentials)
    spreadsheet_key = '1fFfaZWRnfuPimLZsr-j6g4HauErXRiAQyz6tfhuURdo'
    wks = 'Crawl_static'
    spreadsheet = gc.open_by_key(spreadsheet_key)
    values = [df.columns.values.tolist()]
    values.extend(df.values.tolist())
    spreadsheet.values_update(wks, params={'valueInputOption': 'USER_ENTERED'}, body={'values': values})
    print("Done loading_on_spreadsheet\ntime :", time.time() - start, "\n")  # running_time


def handler():
    for url in ['Channel', 'Account', 'Tag']:
        scraping(url)

    read_json_file()

    preprocessing()

    loading_on_spreadsheet()

    print("handler end")


if __name__ == "__main__":
    handler()
