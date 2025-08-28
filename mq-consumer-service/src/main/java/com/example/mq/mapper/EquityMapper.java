package com.example.mq.mapper;

import com.example.mq.model.TradeMessage;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface EquityMapper {


    // 如果暂时不能加 serial_no（只能比较时间）
    @Insert({
            "<script>",
            "INSERT INTO ${table}(biz_dt, customer_no, account_no, ccy, balance) ",
            "VALUES (#{msg.bizDt}, #{msg.customerNo}, #{msg.accountNo}, #{msg.ccy}, #{msg.balance}) ",
            "ON CONFLICT (customer_no, account_no, ccy) DO UPDATE ",
            "SET biz_dt = EXCLUDED.biz_dt, ",
            "    balance  = EXCLUDED.balance ",
            "WHERE EXCLUDED.biz_dt &gt; ${table}.biz_dt",
            "</script>"
    })
    void upsertLatestByTimeOnly(@Param("table") String table, @Param("msg") TradeMessage msg);
}