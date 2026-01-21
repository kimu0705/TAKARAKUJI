from pathlib import Path
import urllib.request as req
import urllib.error
import time
from bs4 import BeautifulSoup
import json
from tqdm.notebook import tqdm

HTML_DIR = Path("..", "data", "html")


def scrape_issue_number():
    """
    結果一覧ページのサイトにアクセスして、issue_numberを取得する関数
    該当ページが存在しない（404）の場合はスキップ
    """
    issue_number_list = []
    url = "https://takarakuji.rakuten.co.jp/backnumber/loto6_past/"
    try:
        html = req.urlopen(url).read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []  # 404のときは空のリストを返してスキップ
        else:
            raise e  # 他のエラーは再スロー
    try:
        time.sleep(1)
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("ul.linkType01 a"):
            href = a.get("href")
            if href.startswith(("/backnumber/loto6/", "/backnumber/loto6_detail/")):
                issue_number_list.append(href.strip("/").split("/")[-1])
    except Exception:
        return []
    return issue_number_list

def read_issue_number():
    """
    issue_numberをjsonで取得する関数
    """
    # このファイル自身の場所（scripts/）
    base_dir = Path(__file__).resolve().parent
    # /boatrace/db/scrape_race_schedule_dict.json
    json_path = base_dir.parent / "db" / "issue_number.json"
    with open(json_path, "r", encoding="utf-8") as f:
        issue_number_list = json.load(f)
    return issue_number_list

def write_html(issue_number_list, save_dir: Path = HTML_DIR):
    """
    ページのhtmlをスクレイピングして、save_dirに保存する関数
    すでにhtmlが存在する場合はスキップされて、新たに取得されたhtmlのパスだけが返ってくる
    """
    html_path_list = []
    for issue_number in tqdm(issue_number_list):
        filepath = save_dir / f"{issue_number}.bin"
        # binファイルがすでに存在する場合はスキップする
        if filepath.is_file():
            print(f"skipped: {issue_number}")
        else:
            if "-" not in issue_number:
                url = f"https://takarakuji.rakuten.co.jp/backnumber/loto6/{issue_number}/"
            else:
                url = f"https://takarakuji.rakuten.co.jp/backnumber/loto6_detail/{issue_number}/"
            html = req.urlopen(url).read()
            time.sleep(1)
            with open(filepath, "wb") as f:
                f.write(html)
            html_path_list.append(filepath)
    return html_path_list