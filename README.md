# 百度MapReduce示例

BMR是Apache Hadoop/Spark托管服务，方便您使用MapReduce、Spark、HBase、Hive、Pig、Kafka等进行大数据处理。

功能

* 灵活设定集群套餐类型、集群规模、服务组件、镜像版本；提供预定义和自定义引导操作服务
* 完全托管的Hadoop/Spark服务；故障自动恢复，按需调整集群规模，实时监控集群、作业的状态
* 引入百度定制优化版本镜像，快速修复开源Hadoop/Spark等组件bug
* 全自动的作业诊断调优；高效的技术支持；专家团队提供优化建议

请访问[百度MapReduce](https://bce.baidu.com/product/bmr.html)了解更多。

## 索引

1. [MapReduce](#MapReduce)

## <a name="MapReduce"></a>MapReduce ##

本示例使用MapReduce分析Web日志，统计每天的请求量为例，介绍如何在开放云平台使用MapReduce。

程序包含Mapper、Reducer和Main入口程序。可以克隆并编译后输出jar文件，创建BMR集群并提交Java作业。

提交完成之后可以在作业列表查看作业运行状态，当作业是“已完成”状态时，可以看到如下结果：

```
03/Oct/2015    139
04/Oct/2015    375
05/Oct/2015    372
06/Oct/2015    114
```
请访问[百度MapReduce在线帮助](https://bce.baidu.com/doc/BMR/QuickGuide.html#MapReduce)了解详细操作步骤。
