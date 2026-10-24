# 26.3.0 旧 Doris 时间编码兼容补丁

本分支基于上游提交 `523ed9d7eda962861d9a8d0a97534df6e337183c`。
该提交的 180 个 base Java 源文件与 Maven Central 的 26.3.0 源码包逐文件一致。
自定义制品使用 `26.3.0-yushu.1`，不覆盖官方版本。
构建目标为 Flink 1.20、Java 8；不升级服务端或更改原有默认解码行为。

## 已复现的真实编码

Doris 2.1.5 的专用测试表同时通过 Flight SQL 和 Thrift 扫描。
每个协议采集 5 行 × 8 个时间字段，共 80 个编码样本。
SQL 原生查询的独立结果作为预期值。
覆盖正常日期、1969 年负 epoch、2300 年远期、1970 年边界及 NULL。
脱敏夹具位于 `flink-doris-connector/flink-doris-connector-base/src/test/resources/datetime/doris-2.1.5-wire.json`。

| 实际字段 | Flight Arrow 声明 | Thrift Arrow 声明 | 实际数值单位 |
| --- | --- | --- | --- |
| DATETIMEV1 | UTF8 | UTF8 | 已格式化字符串 |
| DATETIMEV2(0) | SECOND | MICROSECOND | SECOND |
| DATETIMEV2(1–3) | MILLISECOND | MICROSECOND | MILLISECOND |
| DATETIMEV2(4–6) | MICROSECOND | MICROSECOND | MICROSECOND |

这些时间戳没有 Arrow timezone，但数值表示旧服务端按时区编码的 epoch。
直接按 26.3.0 原生规则解码会出现时差；Thrift 低精度还会落到错误年份。
`enable_date_conversion=true` 时，建表写 `DATETIME` 会转成 V2。
验证 V1 必须显式使用 `DATETIMEV1`，不能由列名推断物理类型。

## 显式启用

仅对已核对编码的源表配置：

```sql
'source.datetime.legacy-time-zone' = 'Asia/Shanghai',
'source.datetime.legacy-units' = 'date_updated:SECOND,precise_time:MICROSECOND'
```

DataStream API 使用 `DorisReadOptions.Builder.setLegacyDateTimeZone(String)` 和
`setLegacyDateTimeUnits(Map<String, String>)`，Properties API 支持相同键名及 Flink map 语法。
字段名必须匹配实际 Arrow 源字段名；所有选中的顶层时间戳 DATETIME/V2 列都需指定单位。
构建时解析时区和单位，冻结为不可变映射；复制和 Java 序列化保留该契约。

- 不配置时区：保持官方 26.3.0 原生行为。
- 配置单位但不配置时区：构建失败。
- 兼容模式下缺少列单位或列携带 Arrow timezone：在批次列绑定时明确失败。
- NULL：保留 NULL；字符串日期：沿用原解析；TIMESTAMPTZ：保持原规则。
- 本次仅修复顶层 DATETIME/V2；不声称覆盖嵌套复杂类型中的时间值。

单位在列批次开始时绑定一次，逐行仅执行整数 epoch 换算。
微秒转换使用 floorDiv/floorMod，负数不丢精度。
传输继续使用 Flight/Arrow，不绕行 JDBC、不转字符串、不使用数值大小启发式。
Flight 与 Thrift 的同步、异步读取器均传递同一组显式选项。

升级服务端后必须重新核对编码，再决定保留或移除兼容配置。
同 Schema 下的后端语义变化无法仅靠 Schema 检测；不要混用编码不同的后端节点。

## 构建与回归

```sh
mvn -B -f flink-doris-connector/pom.xml -Pflink1 \
  -Drevision=26.3.0-yushu.1 -pl flink-doris-connector-flink1 -am \
  -Dtest=DorisLegacyDateTimeTest,DorisLegacyDateTimeOptionsTest,DorisReadOptionsTest,DorisDynamicTableFactoryTest,DorisIncrementalRowBatchTest,TestRowBatch \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest.jvm.args=-Duser.timezone=UTC' package
```

结果制品：`flink-doris-connector/flink-doris-connector-flink1/target/flink-doris-connector-1.20-26.3.0-yushu.1.jar`。
重复更换 JVM 时区可以验证显式时区解码不依赖执行主机默认值。
单元测试不替代真实连接器读取、平台五类案例、进程恢复或性能验收。
