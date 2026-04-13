# 报告补充章节

## Limitations（项目局限性）

1. **运行环境与资源**：流水线在 Windows 伪分布式 HDFS 与 Spark `local[*]` 下验证；Executor 资源与真实 YARN/K8s 集群存在差异，性能与容错表现不能直接外推到生产集群。
2. **存储客户端实现**：HDFS 访问主要通过 `hdfs.cmd` 子进程与 Spark 内置 Hadoop 连接器，未使用长期存活的 Java HDFS 客户端对象；`close_hdfs_client` 在语义上更多体现工程闭环而非释放套接字连接。
3. **数据代表性**：数据集为历史在线零售开票明细，缺少库存、营销触点、退货链路等非开票维度，RFM 与关联规则结论对“全渠道用户价值”仅具参考意义。
4. **算法与统计假设**：FP-Growth 的支持度/置信度阈值对稀疏商品长尾敏感；月度环比/同比未对节假日、促销做控制，解释上存在混淆因素。
5. **结果文件形态**：为便于课程验收，部分高基数结果通过 `coalesce(1)` 汇总为单文件 CSV，存在驱动节点内存压力风险，不适合超大规模无抽样输出。
6. **性能实验设计**：Pandas 基线与 Spark 均读取本地文件系统上的同一 CSV，未在性能脚本中重复测量完整 HDFS I/O 栈；实验体现的是“单机多核 + shuffle 分区”对照，而非严格 TPC 式集群基准。

## Future Work（未来优化方向）

1. **集群化部署**：提供 `spark-submit` 模板与 `yarn-site.xml`/`core-site.xml` 示例，将 `spark_master` 与资源参数改为集群模式可配置项。
2. **增量与分区表**：将 HDFS 存储改为按 `InvoiceDate` 分区的外部表（Hive Metastore 或 Unity Catalog），支持增量追加与分区裁剪扫描。
3. **数据质量契约**：引入 Great Expectations / Deequ 等框架，对列分布、空值率、业务规则做自动化校验与告警。
4. **关联与序列挖掘升级**：在 FP-Growth 之外尝试 PrefixSpan 序列模式、或基于时间的购物篮窗口，提升规则可解释性与可运营性。
5. **可视化与 BI 对接**：将 `results` 中结构化输出对接 Apache Superset / Power BI，并补充固定仪表板模板。
6. **性能实验扩展**：增加 HDFS 读写阶段计时、不同序列化与压缩（Snappy/ZSTD）、以及 Kubernetes 动态 Executor 扩缩容实验，形成完整可发表实验小节。
