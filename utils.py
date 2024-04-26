import re
import zipfile
from io import StringIO, TextIOWrapper
from pathlib import Path
from typing import Union

import pandas as pd


def get_root_folder() -> Path:
  path = Path(__file__).absolute().parent
  return path


def get_data_folder() -> Path:
  path = get_root_folder() / "data"
  return path


def sqldump2csv(
    path_or_buffer: Union[str, Path, StringIO],
    column_quote_char: str = '`'
) -> pd.DataFrame:

  def extract_columns(line):
    i, j = line.find("("), line.find(")")
    if i == -1 or j == -1:
      raise Exception("Columns not found!")
    columns_text = line[i + 1: j]
    columns = []
    for col in columns_text.split(","):
      col = col.strip().strip(column_quote_char).strip()
      columns.append(col)
    return columns

  def extract_values(line):
    line = line.strip()
    ptn = "VALUES ("
    idx = line.find(ptn)
    if idx == -1:
      raise Exception("Invalid SQL insert!")
    else:
      idx += len(ptn)
    values_text = line[idx:].strip(");")
    columns_count = len(extract_columns(line))
    values = []
    pieces = values_text.split(",")
    for i in range(columns_count - 1):
      try:
        values.append(pieces[i].strip("' "))
      except Exception as e:
        print(line)
        print(values_text)
        raise e
    p = ",".join(pieces[columns_count - 1:]).strip("' ")
    values.append(p)
    return values

  fp = path_or_buffer
  if type(path_or_buffer) is str or isinstance(fp, Path):
    print(path_or_buffer)
    fp = open(path_or_buffer, "r", errors="surrogateescape")

  data = []
  columns = None
  for line in fp:
    if columns is None:
      columns = extract_columns(line)
    data.append(extract_values(line))
  df_ret = pd.DataFrame(data=data, columns=columns)
  return df_ret


def get_data_from_zip(zip_path: Path):
  with zipfile.ZipFile(zip_path) as zf:
    with zf.open("smd/thread_bak.sql") as fp:
      input_fp = TextIOWrapper(fp, "utf-8", errors="surrogateescape")
      df_thread = sqldump2csv(input_fp)
    with zf.open("smd/post_bak.sql") as fp:
      input_fp = TextIOWrapper(fp, "utf-8", errors="ignore")
      df_post = sqldump2csv(input_fp)

  return df_thread, df_post


class SizeLimitedFile:
  """用于写入限制大小的文件。

  使用场景：可以使用此类生成大小需要限制在 500 MB 左右的 jsonl 文件。
  """

  def __init__(self, file_size_limit=500_000_000):
    self.file_size_limit = file_size_limit
    self.current_size = 0
    self.fp = None

  def open(self, path):
    self.close()
    self.fp = open(path, "wb")
    self.current_size = 0

  def close(self):
    if self.fp is not None:
      self.fp.close()
      self.fp = None
      self.current_size = 0

  def is_full(self):
    return self.current_size >= self.file_size_limit

  def write(self, data, force=False):
    if self.fp is None:
      raise Exception("No file open for write")
    if self.is_full() and (not force):
      raise Exception(
        f"File size {self.current_size:,} bytes, exceeding limit {self.file_size_limit:,} bytes")

    if type(data) is str:
      data = data.encode()
    self.current_size += self.fp.write(data)

  def writeline(self, data):
    self.write(data)
    self.write(b"\n", force=True)

  def __del__(self):
    self.close()


if __name__ == "__main__":
  sql_path = get_data_folder() / "smd.zip"
  df_thread, df_post = get_data_from_zip(sql_path)

  df_posts = (
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

  print(df_posts.shape)
  print(df_thread.shape)

  df_threads = df_thread.merge(df_posts, on="tid", how="outer")
  print(df_threads.shape)
  print(df_threads[df_threads["author"].isna()].head())


def mask_sensitive_info(text):
  ptn = r"\d{8,}"
  repl = "*"
  return re.sub(ptn, repl, text)