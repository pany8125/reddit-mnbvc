from pydantic import BaseModel, Field
from typing import List
from datetime import datetime, timezone
import json


class Comment(BaseModel):
    floor_id: str = Field(..., alias='楼ID')
    reply: str = Field(..., alias='回复')
    extended_field: str = Field(..., alias='扩展字段')

    class Config:
        populate_by_name = True

class Metadata(BaseModel):
    post_time: str = Field(..., alias='发帖时间')
    reply_count: int = Field(..., alias='回复数')
    extended_field: str = Field(..., alias='扩展字段')

    class Config:
        populate_by_name = True

class Submission(BaseModel):
    id: str = Field(..., alias='ID')
    title: str = Field(..., alias='主题')
    source: str = Field(..., alias='来源')
    created_time: str = Field(..., alias='时间')
    replies: List[Comment] = Field(..., alias='回复')
    metadata: Metadata = Field(..., alias='元数据')

    class Config:
        populate_by_name = True

    @staticmethod
    def format_time(utc_time: int) -> str:
        return datetime.fromtimestamp(utc_time, timezone.utc).strftime('%Y%m%d')

    @staticmethod
    def format_metadata_time(utc_time: int) -> str:
        return datetime.fromtimestamp(utc_time, timezone.utc).strftime('%Y%m%d %H:%M:%S')

# 示例数据
submissions = {
    "id": "dli86",
    "title": "出一张今晚七点贝多芬专场作品音乐会的票，一楼18排4座，原价60出。",
    "created_utc": 1704067200,
    "subreddit": "音乐",
    "score": 1,
    "selftext": "#p 275957 2017-09-24 13:53:31 1 3\n出一张今晚七点贝多芬专场作品音乐会的票，一楼18排4座，原价60出。\n\n#c 1047150 2017-09-24 13:54:21\n[洞主] 有意者可加洞主微信*\n\n#c 1047192 2017-09-24 14:31:48\n[Alice] 搭车转，楼主优先。有要的留言啊\n\n#c 1047510 2017-09-24 17:17:50\n[洞主] 已出"
}

comments = [
    {
        "name": "1",
        "body": "有意者可加洞主微信*",
        "created_utc": 1506248061,
        "author": "洞主"
    },
    {
        "name": "2",
        "body": "搭车转，楼主优先。有要的留言啊",
        "created_utc": 1506250308,
        "author": "Alice"
    },
    {
        "name": "3",
        "body": "已出",
        "created_utc": 1506262670,
        "author": "洞主"
    }
]

# 创建Comment实例列表
comments_list = [
    Comment(
        floor_id=comment["name"],
        reply=comment["body"],
        extended_field=json.dumps({
            "回复人": comment["author"],
            "引用人": comment["author"],
            "回复时间": datetime.fromtimestamp(comment["created_utc"], timezone.utc).strftime('%Y%m%d %H:%M:%S')
        }, ensure_ascii=False)
    )
    for comment in comments
]

# 创建Metadata实例
metadata = Metadata(
    post_time=Submission.format_metadata_time(submissions["created_utc"]),
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
    created_time=Submission.format_time(submissions["created_utc"]),
    replies=comments_list,
    metadata=metadata
)

# 生成JSON字符串
submission_dict = submission.dict(by_alias=True, exclude_none=True)
submission_json = json.dumps(submission_dict, indent=None, ensure_ascii=False)
print(submission_json)
