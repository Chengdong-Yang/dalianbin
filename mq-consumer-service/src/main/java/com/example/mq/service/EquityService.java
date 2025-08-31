package com.example.mq.service;

import com.example.mq.mapper.DailyAggMapper;
import com.example.mq.mapper.EquityMapper;
import com.example.mq.mapper.InboxMapper;
import com.example.mq.model.TradeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;

@Service
@Slf4j
public class EquityService {

    @Autowired
    private EquityMapper equityMapper;

    @Autowired
    private InboxMapper inboxMapper;

    @Autowired
    private DailyAggMapper dailyAggMapper;
    @Autowired
    private  RateCacheService rateCache;

    @Autowired
    private MqMetrics metrics;   // 新增：计数

//    public EquityService(RateCacheService rateCache) {
//        this.rateCache = rateCache;
//    }

    /**
     * @return true 表示应该 ack（正常、重复、脏数据等不需重投）；false 表示应 nAck（临时错误，期望重投）
     */
    public boolean handleMessage(String raw) {
        try {
            TradeMessage msg = TradeMessage.parse(raw);

            // 命中防冲表：重复消息 —— 也算成功，计入 success，并 ack
            if (!inboxMapper.tryInsert(msg.getTxId())) {
                log.warn("击中防冲表，trxID: {}", msg.getTxId());
                metrics.incOk();     // ✅ 重复消息按成功计数
                return true;         // ✅ ack，避免重投
            }

            // 2) 分片表名
            String suffix = shardSuffix(msg.getCustomerNo());  // "01".."16"
            String latestTable = "tb_customer_equity_" + suffix;
            String aggTable    = "agg_cust_daily_" + suffix;

            // 3) 最新快照（抗乱序）
            equityMapper.upsertLatestByTimeOnly(latestTable, msg);

            // 4) 日净变动（D:+，C:-）* 汇率
            BigDecimal signed = "D".equalsIgnoreCase(msg.getCdFlag())
                    ? BigDecimal.valueOf(msg.getAmount())
                    : BigDecimal.valueOf(msg.getAmount()).negate();

            if (signed.signum()!=0) {
                BigDecimal rate = rateCache.getRate(msg.getCcy());
                BigDecimal deltaCny = signed.multiply(rate);
                String ymd = msg.getBizDt().toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE);
                dailyAggMapper.upsertAdd(aggTable, ymd, msg.getCustomerNo(), deltaCny);
            }

            metrics.incOk();   // ✅ 正常消息成功计数
            return true;       // ✅ ack
        } catch (IllegalArgumentException bad) {
            // 文本/字段不合规 —— 业务口径为失败，但不需要重投；直接 ack
            log.warn("invalid msg: {} | reason={}", raw, bad.getMessage());
            metrics.incFail(); // ❗ 失败计数
            return true;       // ✅ 仍然 ack，避免重复消费
        } catch (Exception e) {
            // 临时性异常（DB 短抖、网络问题等） —— 希望重投
            log.error("consume error, msg={}", raw, e);
            metrics.incFail();
            return false;      // ❗ nAck，让 broker 重投
        }
    }

    private static String shardSuffix(String customerNo) {
        long customerId = Long.parseLong(customerNo);
        int shard = (int) (customerId % 16 + 1);
        return String.format("%02d", shard);
    }
}
