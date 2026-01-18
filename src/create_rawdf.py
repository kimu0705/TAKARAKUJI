from pathlib import Path
import pandas as pd
from tqdm.notebook import tqdm
from collections import Counter

RAWDF_DIR = Path("..", "data", "rawdf")

RECENT_NUMBER = 50
NUM_COLS = ['nm1', 'nm2', 'nm3', 'nm4', 'nm5', 'nm6']


def create_results(html_path_list, save_dir: Path = RAWDF_DIR, save_filename = "results.csv"):
    """
    結果ページのhtmlを読み込んで、結果テーブルに加工する関数
    """
    frames = []
    for html_path in html_path_list:
        with open(html_path, "rb") as f:
            html = f.read()
            dfs = pd.read_html(html)

            # 「抽せん日 / 抽選日」を含む table だけ抽出
            indexes = []
            for i, df in enumerate(dfs):
                if df.astype(str).apply(lambda x: x.str.contains("抽せん日|抽選日", na=False)).any().any():
                    indexes.append(i)

            # 条件に合うものがあればそれだけ、なければ全部
            if indexes:
                target_dfs = [dfs[i] for i in indexes]
            else:
                target_dfs = dfs

            # index[回号]を設定、テーブルのを結合
            for df in target_dfs:
                # 「1等」がデータ内に含まれているか判定
                if df.astype(str).apply(lambda x: x.str.contains("1等", na=False)).any().any():
                    df2 = df.drop(index=range(3, 9), errors="ignore")
                    kaigou_id = df2.columns[1]
                    lottery_day = df2.iat[0, 2]
                    insert = df2.columns[0]
                    df2.loc[df2[insert].astype(str).str.strip() == "本数字", insert] = lottery_day
                    bonus_number = df2.iat[2, 1].strip("()")
                    df2['ボーナス数字'] = bonus_number
                    df2 = df2.drop(index=range(0, 1), errors="ignore")
                    df2 = df2.drop(index=range(2, 3), errors="ignore")
                    df2.columns = ['抽せん日', 'nm1', 'nm2', 'nm3', 'nm4', 'nm5', 'nm6', 'ボーナス数字']
                    df2.insert(0, "回号", kaigou_id)
                    df2 = df2.reset_index(drop=True)
                    frames.append(df2)
                else:
                    kaigou_id = '回号'
                    df.columns = ['回号', '抽せん日', 'nm1', 'nm2', 'nm3', 'nm4', 'nm5', 'nm6', 'ボーナス数字']
                    frames.append(df)
    concat_df = pd.concat(frames)
    concat_df = concat_df.reset_index(drop=True)
    # 抽選日を datetime に変換
    concat_df["抽せん日"] = pd.to_datetime(concat_df["抽せん日"])
    # 最新順に並び替え
    concat_df = concat_df.sort_values("抽せん日", ascending=False).reset_index(drop=True)
    concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df

def create_results_distribution_features(results, save_dir: Path = RAWDF_DIR, save_filename = "results_distribution_features.csv"):
    """
    結果テーブルを読み込んで、分布特徴量テーブルに加工する関数
    """
    df = results.sort_values("回号").reset_index(drop=True)
    # 本数字をintに変更
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce").astype("Int64")

    def main_number_zone(rank):
        if rank <= 14:
            return "top"
        elif rank <= 29:
            return "mid"
        else:
            return "low"

    records = []
    for i in range(len(df)):
        if i < RECENT_NUMBER:
            # 直前50回が揃っていない回はスキップ
            continue

        current = df.iloc[i]
        past = df.iloc[i-RECENT_NUMBER:i]

        # 直前50回の出現回数集計
        nums = []
        for _, row in past.iterrows():
            nums.extend([row[c] for c in NUM_COLS])

        freq = Counter(nums)

        # 出ていない数字も含める（1〜43）
        all_nums = list(range(1, 44))
        ranking = sorted(
            all_nums,
            key=lambda x: (-freq.get(x, 0), x)
        )

        rank_map = {num: r+1 for r, num in enumerate(ranking)}

        # 今回の本数字の順位
        current_nums = [current[c] for c in NUM_COLS]
        current_nums = [int(x) for x in current_nums if pd.notna(x)]  # NaN除去 + int化

        ranks = [rank_map[n] for n in current_nums]

        # 順位帯分布
        zones = [main_number_zone(r) for r in ranks]
        rank_top = zones.count("top")
        rank_mid = zones.count("mid")
        rank_low = zones.count("low")

        # 要約指標
        mean_rank = sum(ranks) / len(ranks)
        min_rank  = min(ranks)
        max_rank  = max(ranks)

        records.append({
            "回号": current["回号"],
            "1~14": rank_top,
            "15~29": rank_mid,
            "30~43": rank_low,
            "mean_rank": round(mean_rank, 2),
            "min_rank": min_rank,
            "max_rank": max_rank
        })

    results_distribution_features_df = pd.DataFrame(records)
    # 最新順に並び替え
    results_distribution_features_df = results_distribution_features_df.sort_values("回号", ascending=False).reset_index(drop=True)
    results_distribution_features_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return results_distribution_features_df