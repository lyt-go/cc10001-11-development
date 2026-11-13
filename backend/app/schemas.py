"""接口出入参模型：列表分页、动作结果与各模块的明细结构。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    """登记或修改一条业务记录时提交的字段集合。"""

    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ExportRunRequest(BaseModel):
    """执行一次分片输出：指定任务、批次、管道和输出配置。"""

    task_code: str = ""
    pipeline_code: str | None = None
    export_code: str | None = None
    batch: str = "default"
    records: list[dict[str, Any]] | None = None
    resume: bool = False
    simulate_failure_at_shard: int | None = Field(
        default=None, description="仅用于验证失败恢复：让指定序号的分片写入失败"
    )



class DatasourceEntry(BaseModel):
    """采集源明细结构。"""

    field_0: str | None = None  # 源端编号
    field_1: str | None = None  # 目标域名
    field_2: str | None = None  # 入口URL
    field_3: str | None = None  # 请求方式
    field_4: str | None = None  # 字符编码
    field_5: str | None = None  # 访问频率
    field_6: str | None = None  # 超时秒数
    field_7: str | None = None  # 归属项目

class ParseruleEntry(BaseModel):
    """解析规则明细结构。"""

    field_0: str | None = None  # 规则编号
    field_1: str | None = None  # 页面选择器
    field_2: str | None = None  # 内容选择器
    field_3: str | None = None  # 属性提取器
    field_4: str | None = None  # 列表容错
    field_5: str | None = None  # 空值占位符
    field_6: str | None = None  # 字符清洗正则
    field_7: str | None = None  # 输出字段映射

class ProxyconfigEntry(BaseModel):
    """代理节点明细结构。"""

    field_0: str | None = None  # 节点编号
    field_1: str | None = None  # 代理地址
    field_2: str | None = None  # 代理端口
    field_3: str | None = None  # 代理类型
    field_4: str | None = None  # 地理位置
    field_5: str | None = None  # 带宽上限
    field_6: str | None = None  # 认证凭据
    field_7: str | None = None  # 健康状态

class RatelimiterEntry(BaseModel):
    """限速策略明细结构。"""

    field_0: str | None = None  # 策略编号
    field_1: str | None = None  # 目标域名
    field_2: str | None = None  # 每秒请求数
    field_3: str | None = None  # 每日请求上限
    field_4: str | None = None  # 并发连接数
    field_5: str | None = None  # 冷却秒数
    field_6: str | None = None  # 触发降级阈值
    field_7: str | None = None  # 生效时段

class TaskcrawlEntry(BaseModel):
    """采集任务明细结构。"""

    field_0: str | None = None  # 任务编号
    field_1: str | None = None  # 采集源名称
    field_2: str | None = None  # 规则编号
    field_3: str | None = None  # 调度周期
    field_4: str | None = None  # 增量字段
    field_5: str | None = None  # 批次大小
    field_6: str | None = None  # 起始水位
    field_7: str | None = None  # 截止水位

class PipelineEntry(BaseModel):
    """处理管道明细结构。"""

    field_0: str | None = None  # 管道编号
    field_1: str | None = None  # 数据来源
    field_2: str | None = None  # 字段映射
    field_3: str | None = None  # 去重字段
    field_4: str | None = None  # 清洗表达式
    field_5: str | None = None  # 格式化规则
    field_6: str | None = None  # 输出格式
    field_7: str | None = None  # 异常行策略

class RobotflowEntry(BaseModel):
    """自动化流程明细结构。"""

    field_0: str | None = None  # 流程编号
    field_1: str | None = None  # 入口页面
    field_2: str | None = None  # 页面定位器
    field_3: str | None = None  # 等待条件
    field_4: str | None = None  # 动作序列
    field_5: str | None = None  # 结果捕捉器
    field_6: str | None = None  # 超时秒数
    field_7: str | None = None  # 运行批次

class RetrypolicyEntry(BaseModel):
    """重试策略明细结构。"""

    field_0: str | None = None  # 策略编号
    field_1: str | None = None  # 错误类型
    field_2: str | None = None  # 最大重试次数
    field_3: str | None = None  # 退避算法
    field_4: str | None = None  # 初始间隔秒
    field_5: str | None = None  # 最大间隔秒
    field_6: str | None = None  # 熔断阈值
    field_7: str | None = None  # 恢复探测间隔

class ResultcheckEntry(BaseModel):
    """校验规则明细结构。"""

    field_0: str | None = None  # 校验编号
    field_1: str | None = None  # 目标字段
    field_2: str | None = None  # 非空检查
    field_3: str | None = None  # 类型匹配
    field_4: str | None = None  # 正则表达式
    field_5: str | None = None  # 取值范围
    field_6: str | None = None  # 重复检查
    field_7: str | None = None  # 记录数阈值

class AlertruleEntry(BaseModel):
    """告警规则明细结构。"""

    field_0: str | None = None  # 告警编号
    field_1: str | None = None  # 监控指标
    field_2: str | None = None  # 比较运算符
    field_3: str | None = None  # 阈值数值
    field_4: str | None = None  # 持续周期
    field_5: str | None = None  # 告警级别
    field_6: str | None = None  # 通知渠道
    field_7: str | None = None  # 静默时段

class DiagnoseEntry(BaseModel):
    """诊断报告明细结构。"""

    field_0: str | None = None  # 报告编号
    field_1: str | None = None  # 关联任务
    field_2: str | None = None  # 异常类型
    field_3: str | None = None  # 异常堆栈
    field_4: str | None = None  # 发生时间
    field_5: str | None = None  # 影响行数
    field_6: str | None = None  # 根因分类
    field_7: str | None = None  # 修复建议

class SchedulerEntry(BaseModel):
    """调度计划明细结构。"""

    field_0: str | None = None  # 计划编号
    field_1: str | None = None  # 关联任务
    field_2: str | None = None  # cron表达式
    field_3: str | None = None  # 生效日期
    field_4: str | None = None  # 失效日期
    field_5: str | None = None  # 优先级
    field_6: str | None = None  # 超时分钟
    field_7: str | None = None  # 依赖计划

class ExportconfEntry(BaseModel):
    """输出配置明细结构。"""

    field_0: str | None = None  # 配置编号
    field_1: str | None = None  # 输出格式
    field_2: str | None = None  # 目标路径
    field_3: str | None = None  # 分片阈值
    field_4: str | None = None  # 压缩算法
    field_5: str | None = None  # 列分隔符
    field_6: str | None = None  # 字符编码
    field_7: str | None = None  # 数据源

class LmcfgEntry(BaseModel):
    """登录凭证明细结构。"""

    field_0: str | None = None  # 凭证编号
    field_1: str | None = None  # 目标站点
    field_2: str | None = None  # 认证方式
    field_3: str | None = None  # Cookie内容
    field_4: str | None = None  # 有效期至
    field_5: str | None = None  # 刷新策略
    field_6: str | None = None  # 最近验证时间
    field_7: str | None = None  # 绑定代理

class BlacklistEntry(BaseModel):
    """对抗策略明细结构。"""

    field_0: str | None = None  # 策略编号
    field_1: str | None = None  # 目标域名
    field_2: str | None = None  # UA模拟列表
    field_3: str | None = None  # Referer配置
    field_4: str | None = None  # 随机延迟范围
    field_5: str | None = None  # IP轮换策略
    field_6: str | None = None  # 验证码识别
    field_7: str | None = None  # Cookie种子

class StoragelogEntry(BaseModel):
    """日志记录明细结构。"""

    field_0: str | None = None  # 日志编号
    field_1: str | None = None  # 关联任务
    field_2: str | None = None  # 开始时间
    field_3: str | None = None  # 结束时间
    field_4: str | None = None  # 请求总数
    field_5: str | None = None  # 成功条数
    field_6: str | None = None  # 失败条数
    field_7: str | None = None  # 平均响应毫秒

class TemplatelibEntry(BaseModel):
    """采集模板明细结构。"""

    field_0: str | None = None  # 模板编号
    field_1: str | None = None  # 站点分类
    field_2: str | None = None  # 模板名称
    field_3: str | None = None  # 解析规则集
    field_4: str | None = None  # 请求模板
    field_5: str | None = None  # 分页配置
    field_6: str | None = None  # 适用域名
    field_7: str | None = None  # 生效版本

class DatalabelEntry(BaseModel):
    """标注任务明细结构。"""

    field_0: str | None = None  # 标注编号
    field_1: str | None = None  # 数据集名称
    field_2: str | None = None  # 标注类型
    field_3: str | None = None  # 字段列表
    field_4: str | None = None  # 标注人
    field_5: str | None = None  # 审核人
    field_6: str | None = None  # 总条目数
    field_7: str | None = None  # 完成条目数
