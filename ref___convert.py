import json

import ref___utils as utils


def get_forum_data():
  zip_path = utils.get_data_folder() / "smd.zip"
  df_thread, df_post = utils.get_data_from_zip(zip_path)

  df_post = (
    df_post
    .sort_values("dataline")
    .groupby("tid")
    .agg(
      authors=("author", list),
      datalines=("dataline", list),
      messages=("message", list)
    )
    .reset_index()
  )

  df_data = df_thread.merge(df_post, on="tid", how="outer")

  return df_data


def convert_row(row):
  row = row.to_dict()
  if type(row["author"]) is not str:
    row["author"] = row["authors"][0]
  if type(row["subject"]) is not str:
    row["subject"] = ""

  datalines = []
  for dl in row["datalines"]:
    datalines.append(dl.replace("-", ""))

  replies = []
  idx = 1
  for author, dataline, message in zip(row["authors"], datalines, row["messages"]):
    ext = {
      "回复人": author,
      "回复时间": dataline
    }
    r = {
      "楼ID": f"{idx}",
      "回复": utils.mask_sensitive_info(message),
      "扩展字段": json.dumps(ext)
    }
    replies.append(r)
    idx += 1

  ret = {
    "ID": row["tid"],
    "主题": row["subject"],
    "来源": "神秘岛",
    "时间": datalines[0].split()[0],
    "回复": replies,
    "元数据": {
      "发帖时间": datalines[0],
      "回复数": len(replies),
      "扩展字段": ""
    }
  }
  return ret


def json_dump_bytes(obj):
  """将 json 序列号成 bytes。
  """
  ret = json.dumps(obj, ensure_ascii=False).encode()
  return ret


if __name__ == "__main__":
  df_data = get_forum_data()
  output_path = utils.get_data_folder() / "data"
  if not output_path.exists():
    output_path.mkdir()
  idx = 0
  fp = utils.SizeLimitedFile(file_size_limit=512_000_000)
  fp.open(output_path / f"{idx}.jsonl")
  idx += 1
  for _, row in df_data.iterrows():
    row_bytes = json_dump_bytes(convert_row(row))
    fp.writeline(row_bytes)
    if fp.is_full():
      fp.open(output_path / f"{idx}.jsonl")
      idx += 1
  fp.close()
