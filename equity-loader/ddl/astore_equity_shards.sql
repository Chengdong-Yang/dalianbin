-- =============================
-- GaussDB / openGauss ASTORE 分表 DDL 模板
-- 请根据你们版本的 ASTORE 语法进行调整（示例使用 WITH(storage_type=ASTORE) ）
-- 分表 16 张：tb_customer_equity_01 ~ tb_customer_equity_16
-- 分片规则：客户号取模；单表建议 < 2000 万行
-- =============================

-- 切库
-- CREATE DATABASE contest;
-- \c contest;

DO $$
DECLARE
  i int := 1;
  suffix text;
BEGIN
  WHILE i <= 16 LOOP
    suffix := to_char(i, 'FM00');  -- 得到 01,02,...,16
    EXECUTE
      'CREATE TABLE IF NOT EXISTS tb_customer_equity_' || suffix || E' (
         biz_dt       TIMESTAMP      NOT NULL,
         customer_no  CHAR(10)       NOT NULL,
         account_no   CHAR(18)       NOT NULL,
         ccy          CHAR(3)        NOT NULL,
         balance      NUMERIC(21,2)  NOT NULL
       )';
       -- 如果你们版本需要列存/追加存，请按官方语法在上面括号后追加：
       -- 例如： || ' WITH (orientation=column)'
       -- 分布式集群若支持再加： || ' DISTRIBUTE BY HASH (customer_no)'
    i := i + 1;
  END LOOP;
END $$;

-- 导入完成后再补索引：
DO $$
DECLARE
  i int := 1;
  suffix text;
BEGIN
  WHILE i <= 16 LOOP
    suffix := to_char(i, 'FM00');
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bizdt_' || suffix ||
            ' ON tb_customer_equity_' || suffix || ' (biz_dt)';
        -- 添加唯一约束（新增的）
    EXECUTE 'ALTER TABLE tb_customer_equity_' || suffix ||
            ' ADD CONSTRAINT uk_customer_equity_' || suffix || '_unique' ||
            ' UNIQUE (customer_no, account_no, ccy)';
    i := i + 1;
  END LOOP;
END $$;

-- 单表，不分表
CREATE TABLE IF NOT EXISTS tb_customer_relation (
  customer_no  CHAR(10) NOT NULL,
  csmgr_refno  VARCHAR(7) NOT NULL,
  CONSTRAINT pk_tb_customer_relation PRIMARY KEY (customer_no)
);

CREATE INDEX IF NOT EXISTS idx_relation_mgr
  ON tb_customer_relation (csmgr_refno, customer_no);

COMMENT ON TABLE tb_customer_relation IS '客户-客户经理关系（一对多：经理 -> 多客户）';


-- 防止插重
CREATE TABLE IF NOT EXISTS mq_inbox (
  tx_id VARCHAR(32) PRIMARY KEY
);

-- 聚合表：按客户号分片（customer_no % 16 + 1 -> 01..16）
-- 语义：每个 (ymd, customer_no) 仅 1 行，amount_cny 为该客户当天“净变动(人民币)”的累计值

DO $$
DECLARE
  i int;
  suffix text;
  tbl text;
BEGIN
  FOR i IN 1..16 LOOP
    suffix := lpad(i::text, 2, '0');
    tbl := format('agg_cust_daily_%s', suffix);

    -- 1) 表结构
    EXECUTE format($ddl$
      CREATE TABLE IF NOT EXISTS %I (
        ymd         CHAR(8)      NOT NULL,      -- YYYYMMDD，自然日
        customer_no CHAR(10)     NOT NULL,      -- 左补0的客户号
        amount_cny  NUMERIC(21,2) NOT NULL DEFAULT 0, -- 当日净变动(人民币)，累计值
        CONSTRAINT pk_%I PRIMARY KEY (ymd, customer_no)
      );
    $ddl$, tbl, 'pk_' || tbl);

    -- 2) 客户号索引：便于经理维度聚合时快速过滤
    EXECUTE format($ddl$
      CREATE INDEX IF NOT EXISTS idx_%I_cust
      ON %I (customer_no);
    $ddl$, tbl, tbl);

    -- 3) 可选：为 ymd 单列建索引（如需按日期范围高频扫描时启用）
    -- EXECUTE format($ddl$
    --   CREATE INDEX IF NOT EXISTS idx_%I_ymd
    --   ON %I (ymd);
    -- $ddl$, tbl, tbl);

    -- 4) 备注（可选）
    EXECUTE format($ddl$
      COMMENT ON TABLE %I IS '客户-日净变动(人民币)聚合表：每客户每日一行，消费者增量累计';
    $ddl$, tbl);

  END LOOP;
END$$;

-- ========== 3) 汇率表 ==========
CREATE TABLE IF NOT EXISTS base_cur (
  ccy   CHAR(3)        PRIMARY KEY,
  rate  NUMERIC(21,6)  NOT NULL
);

-- 确保至少有 CNY=1
INSERT INTO base_cur(ccy, rate)
VALUES ('CNY', 1.000000)
ON CONFLICT (ccy) DO NOTHING;

COMMENT ON TABLE base_cur IS '币种对人民币汇率（应用层缓存，定时刷新）';
