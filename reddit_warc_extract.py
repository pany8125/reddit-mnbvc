from datetime import datetime, timezone
import json
import logging
import os
import re
from bs4 import BeautifulSoup

from concurrent.futures import ThreadPoolExecutor, as_completed

from fastwarc.warc import ArchiveIterator, WarcRecordType
import os

from reddit_utils import build_index, fetch_replies
from forum_schema import Comment, Metadata, Submission

# If you want or need to use an HTML parser on this document, you can make this warning go away by filtering it. To do that, run this code before calling the BeautifulSoup constructor:
from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SOURCE = 'reddit'
OUTPUT_DIR = ''
MAX_SIZE = 0

# 配置日志记录
logging.basicConfig(
    filename= SOURCE + '_' + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '_log_file.log',  # 指定日志文件的名称，文件名包含时间
    level=logging.INFO,  # 指定日志级别（INFO、WARNING、ERROR、CRITICAL等）
    format='%(asctime)s [%(levelname)s]: %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S'  # 日期和时间格式
)

# 判断URL的域名中是否包含redd字符，需要先解析到一二三级域名，再判断是否包含redd字符
from urllib.parse import urlparse
def is_reddit_url(url):
    domain = urlparse(url).hostname
    if domain is None:
        return False
    return 'redd' in domain

# 提取不带参数的 URL
def extract_base_url(url):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

# 将文件中的内容清洗为标准的jsonl，处理后写入新文件
def process_file_to_jsonl(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile:
        lines = infile.readlines()

    i = 0
    while i < len(lines):
        # 检查当前行是否只有6个字符以内,并且都是字母和数字
        if len(lines[i].strip()) <= 6 and lines[i].strip().isalnum():
            prev_empty = i > 0 and lines[i - 1].strip() == ''
            next_empty = i < len(lines) - 1 and lines[i + 1].strip() == ''
            # 检查上一行和下一行是否也为空
            if prev_empty and next_empty:
                # 将上上行和下下行拼接
                if i - 2 >= 0 and i + 2 < len(lines):
                    lines[i - 2] = lines[i - 2].rstrip() + lines[i + 2].lstrip()
                # 删除当前行，当前行的上一行，以及下两行
                del lines[i - 1:i + 3]
                i = max(i - 3, 0)  # 回退索引
            else:
                i += 1
        else:
            i += 1

    with open(output_file, 'w', encoding='utf-8') as outfile:
        outfile.writelines(lines)

    # 返回处理后的文件行数
    return len(lines)


# 将文件中的内容清洗为标准的jsonl，处理后写入新文件
def process_file_to_json(input_file, output_file):
    # 打开文件
    warc_file = open(input_file, 'rb')
    # 初始化各种计数
    record_cnt = 0
    response_cnt = 0
    reddit_post = 0
    # 同时打开原始文件和输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in ArchiveIterator(warc_file):
            # 记录数量加1
            record_cnt += 1
            # 只处理response类型的记录
            if record.record_type != WarcRecordType.response:
                continue
            response_cnt += 1
            # 过滤掉状态吗不是200的
            if record.http_headers.status_code != 200:
                # 打印URI和状态码
                logging.debug(f"Filtered: Status Code not 200, URI: {record.headers['WARC-Target-URI']}, Status Code: {record.http_headers.status_code}")
                continue
            # 定义需要过滤的前缀列表
            prefixes = [
                'https://old.reddit.com/',
                'https://www.reddit.com/video/',
                'https://v.redd.it/',
                'https://i.redd.it/',
                'https://b.thumbs.redditmedia.com/',
                'https://preview.redd.it/',
                'https://static01.nyt.com/images/',
                'http://dev.agamitechnologies.com/'
            ]
            # 循环判断是否以指定前缀开头
            for prefix in prefixes:
                if record.headers["WARC-Target-URI"].startswith(prefix):
                    # 打印URI和前缀
                    logging.debug(f"Filtered: URI: {record.headers['WARC-Target-URI']} starts with Prefix: {prefix}")
                    continue
            # 过滤掉 WARC-Target-URI 中基础域名 为 .js .jpg .png .svg .gif .mp4 结尾的
            pattern = r'\.(css|js|jpg|png|svg|gif|mp4|woff2|woff|jpeg)$'
            if re.search(pattern, extract_base_url(record.headers["WARC-Target-URI"])):
                logging.debug(f"Filtered: URI: {record.headers['WARC-Target-URI']}, Filtered Suffix: {pattern}")
                continue
            # 过滤掉不是reddit的record
            if not is_reddit_url(record.headers["WARC-Target-URI"]):
                logging.debug(f"Filtered: URI: {record.headers['WARC-Target-URI']} is not a Reddit URL")
                continue
            reddit_post += 1
            # 打印开始处理的response的URI和长度，以及唯一标识符
            logging.info(f"Processing URI: {record.headers['WARC-Target-URI']}, Content Length: {record.content_length}, Record ID: {record.headers['WARC-Record-ID']}")
            # 读取内容并解码
            content = record.reader.read().decode('utf-8', errors='replace')
            # 使用BeautifulSoup解析HTML，如果处理失败，就跳过，并记录错误对应的record信息
            try:
                soup = BeautifulSoup(content, 'html.parser')
            except Exception as e:
                print(f"Record ID: {record.headers['WARC-Record-ID']}，URI: {record.headers['WARC-Target-URI']}，BeautifulSoup Error: {e}")
                logging.error(f"Record ID: {record.headers['WARC-Record-ID']}，URI: {record.headers['WARC-Target-URI']}，BeautifulSoup Error: {e}")
                continue
            
            for script in soup.find_all('script'):
                if script.string and 'window.___r' in script.string:
                    file_content = script.string
                    # 去除开头的window.___r = 
                    start = file_content.find('{')
                    # 去除末尾的分号
                    end = file_content.rfind('}') + 1
                    json_str = file_content[start:end]
                    # json可能有换行，具体的解析和错误定位放在后面处理
                    f.write(json_str)
                    f.write('\n')
            # 只取出前40个response
            # if need_warc >= 400:
            #     break
    # 关闭文件
    warc_file.close()
    # 返回处理后的文件行数
    return record_cnt, response_cnt, reddit_post


def process_file(full_filename, orig_output_file, jsonl_output_file):
    # 打印文件名和输出文件名
    logging.info(f"Processing File: {full_filename}, Orig output File: {orig_output_file}, Output File: {jsonl_output_file}")
    # 2. 针对每个文件，循环处理，提取json，一个warc文件中所有提取的json放在一个文件中
    record_cnt, response_cnt, reddit_post = process_file_to_json(full_filename, orig_output_file)
    
    # 3. 针对所有的json文件，处理成标准jsonl格式，写入新文件，一个json文件对应一个jsonl文件
    output_jsonl_cnt = process_file_to_jsonl(orig_output_file, jsonl_output_file)
    # TODO针对jsonl文件，需要去重
    
    # 处理jsonl文件到MNBVC格式 TODO

    # 打印处理完成的所有相关信息
    logging.info(f"File: {full_filename}, Record Count: {record_cnt}, Response Count: {response_cnt}, Reddit Post: {reddit_post}, Output JSONL Count: {output_jsonl_cnt}")
    # 将处理结果写入info文件
    return f"File: {full_filename}, Record Count: {record_cnt}, Response Count: {response_cnt}, Reddit Post: {reddit_post}, Output JSONL Count: {output_jsonl_cnt}\n"


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse reddit data.")
    # 非必填参数
    # parser.add_argument("source_dir", type=str, default="D:\\MNBVC\\reddit-mnbvc\\test_warc_dir\\src_file\\", help="文件名或者目录名",nargs="?")
    parser.add_argument("source_dir", type=str, default="\\\XPNas\\network_share\\MNBVC\\reddit_warc_gz", help="文件名或者目录名",nargs="?")
    parser.add_argument("dest_dir",type=str, default="D:\\MNBVC\\reddit-mnbvc\\test_warc_dir\\dest_file\\", help="文件名或者目录名",nargs="?")
    parser.add_argument("-s","--max_size",type=int,default=500 * 1024 * 1024,help="max chunk size")
    args = parser.parse_args()

    SOURCE_DIR = args.source_dir
    OUTPUT_DIR = args.dest_dir
    MAX_SIZE = args.max_size

    # 处理步骤：
    # 1. 循环读取目录下所有warc文件
    # 2. 针对每个文件，循环处理，提取json，一个warc文件中所有提取的json放在一个文件中
    #    2.1. 提取json的逻辑：提取response中的json，过滤掉状态码不是200的，过滤掉指定前缀的URI，过滤掉指定后缀的URI
    # 3. 针对所有的json文件，处理成标准jsonl格式，写入新文件，一个json文件对应一个jsonl文件
    # 4. 针对jsonl文件，处理成MNBVC标准格式，写入新文件，一个jsonl文件对应多个MNBVC文件，每个文件大小不超过500M

    # 创建处理信息
    logging.info(f"Source Directory: {SOURCE_DIR}, Output Directory: {OUTPUT_DIR}, Max Size: {MAX_SIZE}")
    # 建立全局的信息记录文件，将每个文件的最终处理结果写入
    file_name_info = os.path.join(OUTPUT_DIR, 'process_info.txt')
    info_file = open(file_name_info, 'w', encoding='utf-8')
    
    with ThreadPoolExecutor() as executor:
        futures = []
        # 1. 循环读取目录下所有warc文件，包括子目录下的文件
        for root, _, files in os.walk(SOURCE_DIR):
            # 打印当前目录下的根目录和文件
            logging.info(f"Root: {root}, Files: {files}")
            for filename in files:
                # 打印全路径文件名，包括子目录
                full_filename = os.path.join(root, filename)
                # 判断是否为warc或warc.gz文件，如果不是就跳过
                if not full_filename.endswith('.warc') and not full_filename.endswith('.warc.gz'):
                    logging.warning(f"File: {full_filename} is not a WARC file, skipped!")
                    continue
                # 创建原始输出文件，文件名和输入filename一样，只是后缀不一样，原始文件名去掉.warc或者.warc.gz，加上_json.txt
                orig_output_file = os.path.join(OUTPUT_DIR, f'{os.path.splitext(filename)[0]}_json.txt')
                # 创建清洗后的输出文件，文件名和输入filename一样，只是后缀不一样，原始文件名去掉.warc或者.warc.gz，加上_jsonl.txt
                jsonl_output_file = os.path.join(OUTPUT_DIR, f'{os.path.splitext(filename)[0]}_jsonl.txt')
                
                # # 创建异常文件
                err_output_file = os.path.join(OUTPUT_DIR, f'{os.path.splitext(filename)[0]}_err_jsonl.txt')

                # 提交任务到线程池
                futures.append(executor.submit(process_file, full_filename, orig_output_file, jsonl_output_file))
        
        # 等待所有任务完成
        for future in as_completed(futures):
            result = future.result()
            info_file.write(result)

    # 关闭info文件
    info_file.close()
    # 关闭所有日志记录
    logging.shutdown()

# 1.统一重构了下代码
# 2.优化原来对于url的处理逻辑
# 3.优化了日志记录，增加了更多的日志记录
# 4.增加了对于文件处理的异常处理
# 5.修改为多线程执行
