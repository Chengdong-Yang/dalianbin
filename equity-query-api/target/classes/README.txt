# equity-query-api (perf edition)
- Spring Boot 2.7 + MyBatis(+Plus) + PostgreSQL
- DISTINCT ON 优化、应用层并行、汇率内存缓存

Endpoints (port 9000):
- POST /queryAmtByCustNo        -> 命中分表 + 明细 + 内存汇率
- POST /queryAmtByCustNoRange   -> 扫全部分表 + DISTINCT ON + 汇总 (DB端)
- POST /queryAmtByCsmgrRange    -> 当天经理名下

配置:
- equity.shards: 16
- equity.table-prefix: tb_customer_equity_
- equity.parallelism: 16

Build & Run:
mvn -q -U clean package
java -jar target/equity-query-api-1.1.0.jar
