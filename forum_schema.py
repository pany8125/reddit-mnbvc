from pydantic import BaseModel, Field
from typing import List, Dict, Any
from datetime import datetime

# 定义扩展字段模型
class Extension(BaseModel):
    回复人: str = Field(alias='回复人')
    引用人: str = Field(alias='引用人')
    回复时间: datetime = Field(alias='回复时间')

    class Config:
        allow_population_by_field_name = True

# 定义回复模型
class Reply(BaseModel):
    楼ID: str = Field(alias='楼ID')
    回复: str = Field(alias='回复')
    扩展字段: Extension = Field(alias='扩展字段')

    class Config:
        allow_population_by_field_name = True

# 定义元数据中的扩展字段模型
class MetadataExtension(BaseModel):
    标签: int = Field(alias='标签')
    点赞数: int = Field(alias='点赞数')
    原文: str = Field(alias='原文')

    class Config:
        allow_population_by_field_name = True

# 定义元数据模型
class Metadata(BaseModel):
    发帖时间: datetime = Field(alias='发帖时间')
    回复数: int = Field(alias='回复数')
    扩展字段: MetadataExtension = Field(alias='扩展字段')

    class Config:
        allow_population_by_field_name = True

# 定义主模型
class MainModel(BaseModel):
    ID: str = Field(alias='ID')
    主题: str = Field(alias='主题')
    来源: str = Field(alias='来源')
    时间: datetime = Field(alias='时间')
    回复: List[Reply] = Field(alias='回复')
    元数据: Metadata = Field(alias='元数据')

    class Config:
        allow_population_by_field_name = True

# 示例数据
example_data = {
    "ID": "dli86",
    "主题": "出一张今晚七点贝多芬专场作品音乐会的票，一楼18排4座，原价60出。",
    "来源": "reddit",
    "时间": "20240301",
    "回复": [
        {
            "楼ID": "1",
            "回复": "有意者可加洞主微信*",
            "扩展字段": "{\"回复人\": \"洞主\", \"引用人\": \"洞主\", \"回复时间\": \"20170924 13:54:21\"}"
        },
        # ... 其他回复
    ],
    "元数据": {
        # ... 元数据内容
        "扩展字段": "出一张今晚七点贝多芬专场作品音乐会的票，一楼18排4座，原价60出。",
        "回复数": 1,
        "发帖时间": "20240301",
    }
}

# 创建MainModel实例
model_instance = MainModel.model_validate(example_data)

# 输出JSON
print(model_instance.model_dump_json())
