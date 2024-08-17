import json

def build_index(reply_file):
    index = {}
    i = 0
    with open(reply_file, 'r', encoding='utf-8') as f:
        while True:
            i += 1
            if i % 10000 == 0:
                print(f"Processed {i} lines in file {reply_file}")
            position = f.tell()
            line = f.readline()
            if not line:
                break  # 到达文件末尾
            reply = json.loads(line)
            link_id = reply['link_id'].split('_')[1]
            if link_id not in index:
                index[link_id] = []
            index[link_id].append(position)
    return index

def fetch_replies(index, reply_file, post_id):
    positions = index.get(post_id, [])
    replies = []
    with open(reply_file, 'r', encoding='utf-8') as f:
        for position in positions:
            f.seek(position)
            line = f.readline()
            reply = json.loads(line)
            replies.append(reply)
    return replies

# 测试：
# 构建索引
# index = build_index('./src_file/webdevelopment_comments')

# 使用索引获取回复
# post_id = 't3_zsb42b'
# replies = fetch_replies(index, './src_file/webdevelopment_comments', post_id)

# 处理回复
# print(len(replies))
# for reply in replies:
#     print(reply)

# 二分查找
