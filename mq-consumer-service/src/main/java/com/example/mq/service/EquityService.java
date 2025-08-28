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

    public void handleMessage(String raw) {
    try {
        TradeMessage msg = TradeMessage.parse(raw);

        if (!inboxMapper.tryInsert(msg.getTxId())) {
            log.error("击中防冲表，trxID: {}",msg.getTxId());
            return;
        }

        // 2) 分片表名
        String suffix = shardSuffix(msg.getCustomerNo());  // "01".."16"
        String latestTable = "tb_customer_equity_" + suffix;
        String aggTable    = "agg_cust_daily_" + suffix;   // 新增的小表，用于③

        // 3) 更新“最新快照”（抗乱序）
        equityMapper.upsertLatestByTimeOnly(latestTable, msg);

        // 4) 累计“日净变动”（D:+、C:-）× 汇率（缓存）
        BigDecimal signed = "D".equalsIgnoreCase(msg.getCdFlag())
                ? BigDecimal.valueOf(msg.getAmount())
                : BigDecimal.valueOf(msg.getAmount()).negate();

        if (signed.signum()!=0) {
            BigDecimal rate = rateCache.getRate(msg.getCcy());
            BigDecimal deltaCny = signed.multiply(rate);
            String ymd = msg.getBizDt().toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE);
            dailyAggMapper.upsertAdd(aggTable, ymd, msg.getCustomerNo(), deltaCny);
        }

        // 6) 成功计数
        metrics.incOk();

    } catch (IllegalArgumentException bad) {
        // 字段不合规
        log.warn("invalid msg: {} | reason={}", raw, bad.getMessage());
        metrics.incFail();
    } catch (Exception e) {
        // 其它异常
        log.error("consume error, msg={}", raw, e);
        metrics.incFail();
    }
}

    private static String shardSuffix(String customerNo) {
        long customerId = Long.parseLong(customerNo);
        int shard = (int) (customerId % 16 + 1);
        return String.format("%02d", shard);
    }
}
