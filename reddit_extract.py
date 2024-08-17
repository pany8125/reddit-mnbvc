from datetime import datetime, timezone
import json
import logging
import os

from reddit_utils import build_index, fetch_replies
from forum_schema import Comment, Metadata, Submission

SOURCE = 'reddit'
OUTPUT_DIR = ''
MAX_SIZE = 0

# 配置日志记录
logging.basicConfig(
    filename= SOURCE + '_log_file.log',  # 指定日志文件的名称
    level=logging.INFO,  # 指定日志级别（INFO、WARNING、ERROR、CRITICAL等）
    format='%(asctime)s [%(levelname)s]: %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S'  # 日期和时间格式
)

def chk_files(comments_file, submissions_file):
    # 这里添加你的文件处理逻辑
    logging.info(f"check {comments_file} and {submissions_file}")

    with open(submissions_file, 'r', encoding='utf-8') as f:
        # 读取第一行
        line = f.readline().strip('\n').replace('\ufeff', '')
        # 判断第一行是否为一个有效的json
        try:
            json_data = json.loads(line)
        except json.JSONDecodeError:
            line = f.readline()
            logging.warning("JSONDecodeError")
            logging.warning(f"Line 1, error!")
            logging.warning(f"File submissions_file is not a valid json file!")
            return False

    with open(comments_file, 'r', encoding='utf-8') as f:
        # 读取第一行
        line = f.readline().strip('\n').replace('\ufeff', '')
        # 判断第一行是否为一个有效的json
        try:
            json_data = json.loads(line)
        except json.JSONDecodeError:
            line = f.readline()
            logging.warning("JSONDecodeError")
            logging.warning(f"Line 1, error!")
            logging.warning(f"File submissions_file is not a valid json file!")
            return False
    
    # 按jsonl格式处理文件
    process_reddit_files(comments_file, submissions_file)


def process_reddit_files(comments_file, submissions_file):
    # 添加文件处理逻辑
    logging.info(f"Processing {comments_file} and {submissions_file}")

    # 当前时间
    cur_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # 处理提取逻辑
    f_prefix_name = os.path.basename(submissions_file).split('.')[0].split('_')[0]
    output_file_prefix = f'{SOURCE}_{f_prefix_name}_{cur_time}'

    # 文件序号
    file_number = 1
    write_file = open(OUTPUT_DIR + f'{output_file_prefix}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    
    # 打印write_file
    logging.info(f"write_file: {write_file.name}")

    # 构建索引
    index = build_index(comments_file)

    # 这里添加你的文件处理逻辑
    logging.info(f"Processing {submissions_file}")

    with open(submissions_file, 'r', encoding='utf-8') as f:
        for line_number, line in enumerate(f, start=1):
            # 打印迭代信息
            logging.debug(f"Line {line_number}: {line}")
            # 打印写入文件路径
            logging.debug(f"Write file: {write_file.name}")
            try:
                json_data = json.loads(line)
            except json.JSONDecodeError or KeyError:
                logging.error("JSONDecodeError")
                logging.error(f"Line {line_number}, error!")
                exit()
            process_reddit_schema(json_data, index, comments_file, write_file)
            # 如果文件超过500M，就关闭文件，新建文件
            if write_file.tell() > MAX_SIZE:
                write_file.close()
                file_number += 1
                write_file = open(OUTPUT_DIR + f'{output_file_prefix}_{file_number:02}.jsonl', 'w', encoding='utf-8')

def process_reddit_schema(json_data, index, comments_file, write_file):
    # print(f"Processing {json_data['id']}")
    
    # 获取所有回复
    comments = fetch_replies(index, comments_file, json_data["id"])
    # 主贴
    submissions = json_data
    
    # 创建Comment实例列表
    comments_list = [
        Comment(
            # 如果name的key不存在，floor_id就取空
            floor_id=comment["name"] if "name" in comment else "",
            reply=comment["body"],
            extended_field=json.dumps({
                "回复人": comment["author"],
                "引用人": comment["author"],
                "回复时间": datetime.fromtimestamp(int(comment["created_utc"]), timezone.utc).strftime('%Y%m%d %H:%M:%S')
            }, ensure_ascii=False)
        )
        for comment in comments
    ]

    # 创建Metadata实例
    metadata = Metadata(
        post_time=Submission.format_metadata_time(int(submissions["created_utc"])),
        reply_count=len(comments_list),
        extended_field=json.dumps({
            "标签": submissions["subreddit"],
            "点赞数": submissions["score"],
            "原文": submissions["selftext"]
        }, ensure_ascii=False)
    )

    # 创建Submission实例
    submission = Submission(
        id=submissions["id"],
        title=submissions["title"],
        source="reddit",
        created_time=Submission.format_time(int(submissions["created_utc"])),
        replies=comments_list,
        metadata=metadata
    )

    # 生成json
    # 生成JSON字符串
    submission_dict = submission.dict(by_alias=True, exclude_none=True)
    submission_json = json.dumps(submission_dict, indent=None, ensure_ascii=False)
    
    write_file.write(submission_json)
    write_file.write('\n')
    return True

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse reddit data.")
    # 非必填参数
    parser.add_argument("source_dir", type=str, default="G:\\MNBVC\\reddit\\unzip", help="文件名或者目录名",nargs="?")
    parser.add_argument("dest_dir",type=str, default="G:\\MNBVC\\reddit\\dest_dir\\", help="文件名或者目录名",nargs="?")
    parser.add_argument("-s","--max_size",type=int,default=500 * 1024 * 1024,help="max chunk size")
    args = parser.parse_args()
    # directory = "G:\\MNBVC\\reddit\\unzip"
    # directory = "G:\\MNBVC\\reddit-mnbvc\\src_file"
    logging.info(f"print {args.dest_dir}")

    OUTPUT_DIR = args.dest_dir
    MAX_SIZE = args.max_size
    # 获取目录中的所有文件
    files = os.listdir(args.source_dir)
    
    # 过滤并分类文件
    comments_files = sorted([f for f in files if f.endswith('_comments')])
    submissions_files = sorted([f for f in files if f.endswith('_submissions')])
    
    # 检查文件对的数量是否匹配
    if len(comments_files) != len(submissions_files):
        print("Error: Number of comments and submissions files do not match.")
        # 退出
        exit(1)
    
    # 依次处理每一对文件
    for comments_file, submissions_file in zip(comments_files, submissions_files):
        chk_files(os.path.join(args.source_dir, comments_file), os.path.join(args.source_dir, submissions_file))
