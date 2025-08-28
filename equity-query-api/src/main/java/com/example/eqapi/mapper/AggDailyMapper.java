package com.example.eqapi.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;

@Mapper
public interface AggDailyMapper {

    // 返回 (ymd, customer_no, amount_cny) 明细行；由日聚合表直接提供
    List<CustDayRow> listManagerDailyPerCustFromTable(@Param("table") String table,
                                                      @Param("custList") List<String> custList);

    class CustDayRow {
        public String ymd;
        public String custNo;
        public java.math.BigDecimal amount;
    }
}