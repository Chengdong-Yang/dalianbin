# mp-gauss-equity-loader (Spring Boot + MyBatis-Plus)

> **用途**：参赛场景首轮铺底。用 MyBatis-Plus 做工程化与后续 CRUD，真正的大量数据导入走 GaussDB 的 `COPY FROM STDIN`（16 路并发）。

## 快速开始
1. **建表**：执行 `ddl/astore_equity_shards.sql`（按你们 ASTORE 语法微调）。
2. **配置数据库**：修改 `src/main/resources/application.yml` 中的 `spring.datasource.*`。
3. **准备文件**：UTF-8 文本，字段以 `|` 分隔，示例：
   ```text
   2025-06-01 21:09:01|0123456781|123456789012345000|CNY|1000.29
   ```
4. **设置环境变量**：
   ```bash
   export FILE_PATH=/data/in
   export FILE_NAME_EQUITY=equity.txt
   ```
5. **打开装载开关**：`application.yml` 中将 `loader.enabled` 改成 `true`。
6. **构建运行**：
   ```bash
   mvn -DskipTests package
   java -Xms2g -Xmx2g -jar target/mp-gauss-equity-loader-1.0.0.jar
   ```

坏数据会输出到同目录的 `equity.txt.bad`。导入完成后，请执行 `ANALYZE` 更新统计信息。

## 代码结构
- `CopyLoadServiceImpl`：**核心装载器**，带中文注释，说明了流式队列、分片路由、Pipe + COPY 的实现细节。
- `TableRouteContext` + `MybatisPlusConfig`：**动态表名路由**，支持后续使用 MP 查询/写入分表。
- `EquityMapper`：通用 Mapper，如需备选“批量 INSERT”，可在 `mapper/EquityMapper.xml` 增加多值插入语句。

## 常见调优
- 装载窗口可临时调整：`synchronous_commit=off`、适当增大 WAL/内存；导完再恢复。
- 导入阶段不建索引，导完批量补索引再 `ANALYZE`。
- 并行度（`loader.shards`）建议与分表一致；I/O 足够时可加大。

## 版本
- JDK 1.8
- Spring Boot 2.7.x
- MyBatis-Plus 3.5.x
- PostgreSQL Driver（Gauss 兼容）
  export FILE_PATH=/Users/yangchengdong/Downloads/mp-gauss-equity-loader-with-comments/src/main/resources/scripts
  export FILE_NAME_EQUITY=file_name_equity.txt
  export FILE_NAME_RELATION=file_name_relation.txt
