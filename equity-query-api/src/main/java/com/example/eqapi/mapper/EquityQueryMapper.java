package com.example.eqapi.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.math.BigDecimal;
import java.util.List;
import com.example.eqapi.mapper.EquityQueryMapper.HoldingRow;

@Mapper
public interface EquityQueryMapper {
  // 单客户：取快照明细
  List<HoldingRow> selectHoldingsFromTable(@Param("table") String table,
                                           @Param("custNo") String custNo);

  // 区间合计：当前快照求和（余额×汇率）
  BigDecimal sumRangeFromTable(@Param("table") String table,
                               @Param("min") String min,
                               @Param("max") String max);

  class HoldingRow {
    public String accountNo;
    public String ccy;
    public java.math.BigDecimal balance;
    public String bizDt; // 已格式化
  }
}
