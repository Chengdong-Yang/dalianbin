-- 为所有分表 01..16 分别创建（请替换表名循环执行）

-- 单客户：按 (customer_no, account_no, ccy) 取最新
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_equity_cust_acc_ccy_bizdt
ON public.tb_customer_equity_01 (customer_no, account_no, ccy, biz_dt DESC)
INCLUDE (balance, ccy);

-- 区间合计：customer_no 区间 + 最新
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_equity_cust_bizdt
ON public.tb_customer_equity_01 (customer_no, biz_dt DESC)
INCLUDE (account_no, ccy, balance);

-- 当天经理名下：按日期过滤加速
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_equity_bizdt_cust
ON public.tb_customer_equity_01 (biz_dt, customer_no)
INCLUDE (account_no, ccy, balance);

-- 关系表
CREATE INDEX IF NOT EXISTS idx_rel_mgr ON tb_customer_relation (csmgr_refno, customer_no);

-- 汇率表主键（如未建）
ALTER TABLE base_cur ADD CONSTRAINT pk_base_cur PRIMARY KEY (ccy);

-- 可选：提高当前会话并行与内存（按需设置）
-- SET max_parallel_workers_per_gather = 2;
-- SET work_mem = '64MB';
