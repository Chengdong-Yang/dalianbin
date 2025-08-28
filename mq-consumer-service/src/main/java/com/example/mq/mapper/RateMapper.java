package com.example.mq.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface RateMapper {

  @org.apache.ibatis.annotations.Select(
          "SELECT rate FROM base_cur WHERE UPPER(ccy)=UPPER(#{ccy}) LIMIT 1")
  BigDecimal findRate(@Param("ccy") String ccy);

  @org.apache.ibatis.annotations.Select(
          "SELECT ccy, rate FROM base_cur")
  List<RateRow> findAll();

  class RateRow {
    public String ccy;
    public BigDecimal rate;
  }
}
