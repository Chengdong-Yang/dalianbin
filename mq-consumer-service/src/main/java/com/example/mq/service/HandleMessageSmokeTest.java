package com.example.mq.service;


import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

@Component
@ConditionalOnProperty(prefix = "smoketest", name = "enabled", havingValue = "true", matchIfMissing = false)
public class HandleMessageSmokeTest {

    @Autowired
    EquityService consumer;
    @Autowired
    JdbcTemplate jdbc; // 方便查表断言

    @PostConstruct // 应用启动完成后自动执行
    public void init() {
        feedAndCheck();
    }

    @Test
    public void feedAndCheck() {
        String[] lines = {
                "2025-06-01 21:00:01|TX10000001|123|123456789012345000|D|CNY|100.00|1100.00",
                "2025-06-01 21:05:00|TX10000002|123|123456789012345000|C|CNY|50.00|1050.00",
                "2025-06-01 21:04:00|TX10000003|123|123456789012345000|D|CNY|20.00|1070.00",
                "2025-06-01 21:05:00|TX10000002|123|123456789012345000|C|CNY|50.00|1050.00",
                "2025-06-01 21:09:01|TX20000001|4567890123|123456789012345001|D|EUR|98.20|100.29",
                "2025-06-01 21:09:02|TX20000002|4567890123|123456789012345001|C|EUR|10.00|90.29",
                "2025-06-01 21:09:03|TX20000003|4567890123|123456789012345002|D|EUR|200.00|1200.00",
                "2025-06-01 21:09:04|TX20000004|4567890123|123456789012345002|C|EUR|50.00|1150.00",
                "2025-06-02 09:00:00|TX30000001|9999999999|123456789012345100|D|CNY|1000.00|1000.00",
                "2025-06-02 09:00:30|TX30000002|9999999999|123456789012345100|C|CNY|200.00|800.00",
                "2025-06-02 09:00:35|TX30000003|9999999999|123456789012345101|D|EUR|300.00|300.00",
                "2025-06-02 09:00:40|TX30000004|9999999999|123456789012345101|C|EUR|100.00|200.00",
                "2025-06-02 09:00:50|TX30000005|9999999999|123456789012345101|D|EUR|60.00|260.00"
        };
        for (String raw : lines) consumer.handleMessage(raw);

        // 快照 12 分片：0000000123 (CNY 1050.00)
        BigDecimal b1 = jdbc.queryForObject(
                "SELECT balance FROM tb_customer_equity_12 WHERE customer_no='0000000123' AND account_no='123456789012345000' AND ccy='CNY'",
                BigDecimal.class);
        assertEquals(new BigDecimal("1050.00"), b1);

        // 快照 16 分片：9999999999 账户两条
        BigDecimal b2 = jdbc.queryForObject(
                "SELECT balance FROM tb_customer_equity_16 WHERE customer_no='9999999999' AND account_no='123456789012345100' AND ccy='CNY'",
                BigDecimal.class);
        assertEquals(new BigDecimal("800.00"), b2);

        BigDecimal b3 = jdbc.queryForObject(
                "SELECT balance FROM tb_customer_equity_16 WHERE customer_no='9999999999' AND account_no='123456789012345101' AND ccy='EUR'",
                BigDecimal.class);
        assertEquals(new BigDecimal("260.00"), b3);

        // 聚合 12：20250601 两个客户
        BigDecimal a1 = jdbc.queryForObject(
                "SELECT amount_cny FROM agg_cust_daily_12 WHERE ymd='20250601' AND customer_no='0000000123'",
                BigDecimal.class);
        assertEquals(new BigDecimal("70.00"), a1); // +100 -50 +20

        BigDecimal a2 = jdbc.queryForObject(
                "SELECT amount_cny FROM agg_cust_daily_12 WHERE ymd='20250601' AND customer_no='4567890123'",
                BigDecimal.class);
        assertEquals(new BigDecimal("1857.96"), a2); // (98.20-10+150)*7.80

        // 聚合 16：20250602 9999999999
        BigDecimal a3 = jdbc.queryForObject(
                "SELECT amount_cny FROM agg_cust_daily_16 WHERE ymd='20250602' AND customer_no='9999999999'",
                BigDecimal.class);
        assertEquals(new BigDecimal("2828.00"), a3); // +800 CNY + 260*7.80
    }
}